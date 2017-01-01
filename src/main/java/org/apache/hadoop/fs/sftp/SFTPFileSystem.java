package org.apache.hadoop.fs.sftp;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.util.Progressable;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SFTPException;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;
import ch.ethz.ssh2.ServerHostKeyVerifier;
import ch.ethz.ssh2.InteractiveCallback;
import ch.ethz.ssh2.sftp.ErrorCodes;

/**
 * Based on implementation posted on Hadoop JIRA. Using Ganymede SSH lib for
 * improved performance.
 * 
 * @see https://issues.apache.org/jira/browse/HADOOP-5732
 * @author wnagele
 */
public class SFTPFileSystem extends FileSystem {
	
	public static final Log LOG = LogFactory.getLog(SFTPFileSystem.class);
	  
	private final int MAX_BUFFER_SIZE = 32768;
	private final int DEFAULT_PORT = 22;
	private final String DEFAULT_KEY_FILE = "${user.home}/.ssh/id_rsa";

	private final String PARAM_BUFFER_SIZE = "io.file.buffer.size";
	private final String PARAM_HOST = "fs.sftp.host";
	private final String PARAM_PORT = "fs.sftp.port";
	private final String PARAM_USER = "fs.sftp.user";
	private final String PARAM_PASSWORD = "fs.sftp.password";
	private final String PARAM_KEY_FILE = "fs.sftp.key.file";
	private final String PARAM_KEY_PASSWORD = "fs.sftp.key.password";

	private Configuration conf;
	private URI uri;
	private SFTPv3ClientWrapper client;
	private Connection connection;	
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		Logger.getLogger("ch.ethz.ssh2").setLevel(Level.OFF);

		this.uri = uri;
		this.conf = conf;

		conf.unset(PARAM_HOST);
		conf.unset(PARAM_PORT);
		conf.unset(PARAM_USER);
		conf.unset(PARAM_PASSWORD);
		
		// If no explicit buffer was set use the maximum.
		// Also limit the buffer to the maximum value.
		int bufferSize = conf.getInt(PARAM_BUFFER_SIZE, -1);
		if (bufferSize > MAX_BUFFER_SIZE || bufferSize == -1)
			conf.setInt(PARAM_BUFFER_SIZE, MAX_BUFFER_SIZE);

		String host = uri.getHost();
		if (host != null)
			conf.set(PARAM_HOST, host);

		int port = uri.getPort();
		if (port != -1)
			conf.setInt(PARAM_PORT, port);

		String userInfo = uri.getUserInfo();
		if (userInfo != null) {
			if (!userInfo.contains(":")) {
				conf.set(PARAM_USER, userInfo);
			} else {
				String[] userAndPassword = userInfo.split(":", 2);
				String user = userAndPassword[0];
				conf.set(PARAM_USER, user);
				String password = userAndPassword[1];
				conf.set(PARAM_PASSWORD, password);
			}
		}

		setConf(conf);

		try {
			connect();
		} catch (IOException e) {
			// Ensure to close down connections if we fail during initialization
			close();
			throw e;
		}
	}

	protected void connect() throws IOException {
		
		if (client == null || !client.isConnected()) {
			
			connection = getConnection();
			LOG.info("Creating client");
			client = new SFTPv3ClientWrapper(connection);
			//client.setRequestParallelism(1);
		}
	}
	
	protected Connection getConnection() throws IOException {
		String host = conf.get(PARAM_HOST);
		int port = conf.getInt(PARAM_PORT, DEFAULT_PORT);
		String key = conf.get(PARAM_KEY_FILE, DEFAULT_KEY_FILE);
		String keyPassword = conf.get(PARAM_KEY_PASSWORD);
		String user = conf.get(PARAM_USER);
		final String password = conf.get(PARAM_PASSWORD);
		Connection connection = new Connection(host, port);

		connection.connect(new ServerHostKeyVerifier() {
			@Override
			public boolean verifyServerHostKey(String hostname, int port,
					String serverHostKeyAlgorithm, byte[] serverHostKey)
					throws Exception {

				return true;
			}
		});

		if (password != null) {
			if (connection.isAuthMethodAvailable(user, "password")) {
				connection.authenticateWithPassword(user, password);
			} else if (connection.isAuthMethodAvailable(user, "keyboard-interactive")) {
				connection.authenticateWithKeyboardInteractive(user, new InteractiveCallback() {
					@Override
					public String[] replyToChallenge(String name, String instruction, int numPrompts, String[] prompt, boolean[] echo) throws Exception {
						switch (prompt.length) {
							case 0:
								return new String[0];
							case 1:
								return new String[] { password };
						}
						throw new IOException("Cannot proceed with keyboard-interactive authentication. Server requested " + prompt.length + " challenges, we only support 1.");
					}
				});
			} else {
				throw new IOException("Server does not support any of our supported password authentication methods");
			}
		} else {
			connection.authenticateWithPublicKey(user, new File(key), keyPassword);
		}
		
		return connection;		
	}
	
	protected SFTPv3ClientWrapper getClient() throws IOException {
		LOG.info("Creating specific client");
		SFTPv3ClientWrapper client = new SFTPv3ClientWrapper(getConnection());
		client.setRequestParallelism(1);
		return client;
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (client != null && client.isConnected()) {
			LOG.info("Closing client");
			client.close();
		}
		if (connection != null)
			connection.close();
	}

	@Override
	public FSDataInputStream open(Path file) throws IOException {
		SFTPInputStream is = openInternal(file);
		return new FSDataInputStream(is);
	}

	@Override
	public FSDataInputStream open(Path file, int bufferSize) throws IOException {
		SFTPInputStream is = openInternal(file);
		return new FSDataInputStream(new BufferedFSInputStream(is, bufferSize));
	}

	private SFTPInputStream openInternal(Path file) throws IOException {
		if (getFileStatus(file).isDir())
			throw new IOException("Path " + file + " is a directory.");

		LOG.info("Opening file: " + file.getName());
		
		String path = file.toUri().getPath();
		SFTPv3FileHandle handle = getClient().openFileRO(path);
		SFTPInputStream is = new SFTPInputStream(handle, statistics);
		return is;
	}

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		return createInternal(file, permission, overwrite,
				SFTPv3Client.SSH_FXF_CREAT | SFTPv3Client.SSH_FXF_WRITE
						| SFTPv3Client.SSH_FXF_TRUNC);
	}

	@Override
	public FSDataOutputStream append(Path file, int bufferSize,
			Progressable progress) throws IOException {
		return createInternal(file, null, true, SFTPv3Client.SSH_FXF_WRITE
				| SFTPv3Client.SSH_FXF_APPEND);
	}

	protected FSDataOutputStream createInternal(Path file,
			FsPermission permission, boolean overwrite, int flags)
			throws IOException {
		if (exists(file) && !overwrite)
			throw new IOException("File " + file + " exists");

		Path parent = file.getParent();
		if (!exists(parent))
			mkdirs(parent);

		SFTPv3FileAttributes attrs = null;
		if (permission != null) {
			attrs = new SFTPv3FileAttributes();
			attrs.permissions = new Short(permission.toShort()).intValue();
		}

		String path = file.toUri().getPath();
		SFTPv3FileHandle handle = getClient().openFile(path, flags, attrs);
		SFTPOutputStream os = new SFTPOutputStream(handle, statistics);
		return new FSDataOutputStream(new BufferedOutputStream(os), statistics);
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission)
			throws IOException {
		if (!exists(file)) {
			Path parent = file.getParent();
			if (parent == null || mkdirs(parent, permission)) {
				String path = file.toUri().getPath();
				client.mkdir(path, permission.toShort());
			}
		}
		return true;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		String oldPath = src.toUri().getPath();
		String newPath = dst.toUri().getPath();
		client.mv(oldPath, newPath);
		return true;
	}

	@Override
	public boolean delete(Path file, boolean recursive) throws IOException {
		String path = file.toUri().getPath();
		if (!getFileStatus(file).isDir()) {
			client.rm(path);
			return true;
		}

		FileStatus[] dirEntries = listStatus(file);
		if (dirEntries != null && dirEntries.length > 0 && !recursive)
			throw new IOException("Directory: " + file + " is not empty.");

		for (FileStatus dirEntry : dirEntries)
			delete(dirEntry.getPath(), recursive);

		client.rmdir(path);
		return true;
	}

	@Override
	public boolean delete(Path file) throws IOException {
		return delete(file, false);
	}

	@Override
	public void setTimes(Path file, long mtime, long atime) throws IOException {
		FileStatus status = getFileStatus(file);
		String path = status.getPath().toUri().getPath();
		SFTPv3FileAttributes attrs = client.stat(path);
		attrs.mtime = new Long(mtime / 1000L).intValue();
		attrs.atime = new Long(atime / 1000L).intValue();
		client.setstat(path, attrs);
	}

	@Override
	public FileStatus getFileStatus(Path file) throws IOException {		
		file = file.makeQualified(this);
		if (file.getParent() == null){
			LOG.info("getFileStatus1 ifnull- " + file.toUri().toString() + " len=0");
			return new FileStatus(0, true, 1, getDefaultBlockSize(), 0,
					new Path("/").makeQualified(this));
		}
		for (int attempt = 0; true; attempt++) {
			try {
				String path = file.toUri().getPath();
				LOG.info("getFileStatus2 - " + file.toUri().toString());
				SFTPv3FileAttributes attrs = client.stat(path);
				LOG.info("getFileStatus3 - " + file.toUri().toString() + " len=" + attrs.size);
				//LOG.info("getFileStatus2 - filesystem=" + file.getFileSystem(conf).toString());
				FileStatus status = getFileStatus(attrs, file);
				return status;
						
			} catch (SFTPException e) {
				LOG.info("caught exception, retrying: ", e);
				if (attempt >3) {
					if (e.getServerErrorCode() == ErrorCodes.SSH_FX_NO_SUCH_FILE)
						throw new FileNotFoundException(file.toString());
					throw e;
				}
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) { }
		}
	}

	private FileStatus getFileStatus(SFTPv3FileAttributes attrs, Path file)
			throws IOException {
		long length = 0;
		if (attrs.size != null)
			length = attrs.size;
		boolean isDir = attrs.isDirectory();
		long modTime = 0;
		if (attrs.mtime != null)
			modTime = new Integer(attrs.mtime).longValue() * 1000L;
		long accessTime = 0;
		if (attrs.atime != null)
			accessTime = new Integer(attrs.atime).longValue() * 1000L;
		FsPermission permission = null;
		if (attrs.permissions != null)
			permission = new FsPermission(new Integer(
				attrs.permissions).shortValue());
		String user = null; 
		if (attrs.uid != null)
			user = Integer.toString(attrs.uid);
		String group  = null; 
		if (attrs.gid != null)
			group = Integer.toString(attrs.gid);
		return new FileStatus(length, isDir, 1, getDefaultBlockSize(), modTime,
				accessTime, permission, user, group, file);
	}

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		FileStatus fileStat = getFileStatus(path);
		if (!fileStat.isDir()) {
			LOG.info("listStatus1 - filesystem=" + fileStat.getPath().getFileSystem(conf).toString());
			return new FileStatus[] { fileStat };
		}

		String strPath = path.toUri().getPath();

		List<SFTPv3DirectoryEntry> sftpFiles = client.ls(strPath);
		ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>(
				sftpFiles.size());
		for (SFTPv3DirectoryEntry sftpFile : sftpFiles) {
			String filename = sftpFile.filename;
			if (!"..".equals(filename) && !".".equals(filename)){
				Path file = new Path(path,
						filename).makeQualified(this);
				fileStats.add(getFileStatus(sftpFile.attributes, file));
				LOG.info("listStatus2 - file=" + file.toUri().toString());
				LOG.info("listStatus2 - filesystem=" + file.getFileSystem(conf).toString());
			}
		}
		return fileStats.toArray(new FileStatus[0]);
	}

	@Override
	public boolean exists(Path file) {
		try {
			return getFileStatus(file) != null;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public boolean isFile(Path file) throws IOException {
		return !getFileStatus(file).isDir();
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public void setWorkingDirectory(Path workDir) {
	}

	@Override
	public Path getWorkingDirectory() {
		return null;
	}

	@Override
	public long getDefaultBlockSize() {
		return getConf().getLong("fs.sftp.block.size", 64 * 1024 * 1024);
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		LOG.info("getFileBlockLocations - start = " + start + " , len = " + len);
		LOG.info("getFileBlockLocations: path=" + file.getPath().toUri().toString() + " len=" + file.getLen() + " blocksize=" + file.getBlockSize() + " isdir=" + file.isDir());
		return super.getFileBlockLocations(file, start, len);
	}

	static class SFTPv3ClientWrapper extends SFTPv3Client {

		protected final Connection connection;
		
		public SFTPv3ClientWrapper(Connection connection) throws IOException {
			super(connection);
			this.connection = connection;
		}
		
		public Connection getConnection() {
			return this.connection;
		}
	}
	
}