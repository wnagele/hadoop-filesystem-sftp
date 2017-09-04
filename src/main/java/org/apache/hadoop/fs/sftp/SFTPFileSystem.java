package org.apache.hadoop.fs.sftp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import org.apache.hadoop.util.Progressable;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.InteractiveCallback;
import ch.ethz.ssh2.SFTPException;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;
import ch.ethz.ssh2.ServerHostKeyVerifier;
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
	private final String PARAM_MAX_ATTEMPTS = "fs.sftp.maxattempts";
	private final String PARAM_RETRY_SLEEP = "fs.sftp.retrysleep";

	private Configuration conf;
	private URI uri;
	private SFTPv3ClientWrapper client;
	private Connection connection;

	private int maxAttempts = 5;
	private int retrySleep = 500; 

	
	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
		
		String url = "sftp://telusupload:jGr0%2fBEiYjotsjBVtt5rRuhZb3g%3d@104.236.67.127:22/incoming/telus_report_activations_2017_03_31.csv";
		URI uri = new URI(url);
		SFTPFileSystem fs = new SFTPFileSystem();
		fs.initialize(uri, new Configuration());
		FSDataInputStream in = fs.open(new Path(url));
		byte[] buffer = new byte[32768];
		System.out.println(in.read(0, buffer, 0, 32768));
		System.out.println(in.read(32768, buffer, 0, 32768));
		//IOUtils.copy(in,out);
		in.close();
		url = "sftp://a:b@c:22/Processed/20151121-Downloads.csv";
		fs.getFileStatus(new Path(url));
		in = fs.open(new Path(url));
		buffer = new byte[32768];
		System.out.println(in.read(0, buffer, 0, 32768));
		System.out.println(in.read(32768, buffer, 0, 32768));
		//IOUtils.copy(in,out);
		in.close();
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		Logger.getLogger("ch.ethz.ssh2").setLevel(Level.OFF);

		this.uri = uri;
		this.conf = conf;

		maxAttempts = conf.getInt(PARAM_MAX_ATTEMPTS, maxAttempts);
		retrySleep = conf.getInt(PARAM_RETRY_SLEEP, retrySleep);

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
			closeClient();
			connection = createConnection();
			LOG.info("Creating client");
			client = new SFTPv3ClientWrapper(connection);
			//client.setRequestParallelism(1);
		}
	}
	
	protected SFTPv3ClientWrapper getClient() throws IOException {
		connect();
		return client;		
	}
	
	protected Connection createConnection() throws IOException {
		LOG.info("Creating connection");
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
	
	protected SFTPv3ClientWrapper createClient() throws IOException {
		LOG.info("Creating client for file stream");
		SFTPv3ClientWrapper client = new SFTPv3ClientWrapper(createConnection());
		client.setRequestParallelism(1);
		return client;
	}

	@Override
	public void close() throws IOException {
		super.close();
		LOG.info("Closing client");
		closeClient();
	}

	private void closeClient() throws IOException {
		if (client != null) {
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
		if (getFileStatus(file, true).isDir())
			throw new IOException("Path " + file + " is a directory.");

		LOG.info("Opening file for read: " + file.toString());
		
		String path = file.toUri().getPath();
		SFTPv3FileHandle handle = createClient().openFileRO(path);
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
		if (exists(file, false) && !overwrite)
			throw new IOException("File " + file + " exists");

		LOG.info("Opening file for write: " + file.toString());
		
		Path parent = file.getParent();
		if (!exists(parent, true))
			mkdirs(parent);

		SFTPv3FileAttributes attrs = null;
		if (permission != null) {
			attrs = new SFTPv3FileAttributes();
			attrs.permissions = new Short(permission.toShort()).intValue();
		}

		String path = file.toUri().getPath();
		SFTPv3FileHandle handle = createClient().openFile(path, flags, attrs);
		SFTPOutputStream os = new SFTPOutputStream(handle, statistics);
		return new FSDataOutputStream(new BufferedOutputStream(os), statistics);
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission)
			throws IOException {
		LOG.info("creating dir: " + file.toString());
		if (!exists(file, false)) {
			Path parent = file.getParent();
			if (parent == null || mkdirs(parent, permission)) {
				String path = file.toUri().getPath();
				getClient().mkdir(path, permission.toShort());
			}
		}
		return true;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		String oldPath = src.toUri().getPath();
		String newPath = dst.toUri().getPath();
		
		LOG.info("moving file: " + oldPath + " -> " + newPath);

		delete(dst, false);
		for (int attempt = 1; true; attempt++) {
			try {
				getClient().mv(oldPath, newPath);	
				return true;		
			} catch (SFTPException e) {
				if (attempt >= maxAttempts) {
					throw e;
				} else
					LOG.debug("caught exception in attempt " + attempt + ", retrying: ", e);
			}
			try {
				Thread.sleep(retrySleep);
			} catch (InterruptedException e) { }
		}		
	}

	@Override
	public boolean delete(Path file, boolean recursive) throws IOException {
		if (!exists(file, false))
			return false;
		String path = file.toUri().getPath();
		LOG.info("deleting file: " + path);
		if (!getFileStatus(file, false).isDir()) {
			getClient().rm(path);
			return true;
		}

		FileStatus[] dirEntries = listStatus(file);
		if (dirEntries != null && dirEntries.length > 0 && !recursive)
			throw new IOException("Directory: " + file + " is not empty.");

		for (FileStatus dirEntry : dirEntries)
			delete(dirEntry.getPath(), recursive);

		getClient().rmdir(path);
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
		SFTPv3FileAttributes attrs = getClient().stat(path);
		attrs.mtime = new Long(mtime / 1000L).intValue();
		attrs.atime = new Long(atime / 1000L).intValue();
		getClient().setstat(path, attrs);
	}

	@Override
	public FileStatus getFileStatus(Path file) throws IOException {		
		return getFileStatus(file, true);
	}

	private FileStatus getFileStatus(Path file, boolean retryIfNotExists) throws IOException {		
		file = file.makeQualified(this);
		if (file.getParent() == null){
			return new FileStatus(0, true, 1, getDefaultBlockSize(), 0,
					new Path("/").makeQualified(this));
		}
		for (int attempt = 1; true; attempt++) {
			try {
				String path = file.toUri().getPath();
				LOG.info("getFileStatus - calling for stats - " + file.toUri().toString());
				SFTPv3FileAttributes attrs = getClient().stat(path);
				LOG.info("getFileStatus - getting file status from attributes" + file.toUri().toString() + " len=" + attrs.size);
				FileStatus status = getFileStatus(attrs, file);
				return status;
						
			} catch (SFTPException e) {
				if (!retryIfNotExists || attempt >= maxAttempts) {
					if (e.getServerErrorCode() == ErrorCodes.SSH_FX_NO_SUCH_FILE) {
						LOG.info("getFileStatus - file does not exist - " + file.toUri().toString());
						throw new FileNotFoundException(file.toString());
					}
					LOG.debug("getFileStatus - couldn't complete request.", e);
					throw e;
				} else
					LOG.debug("caught exception in attempt " + attempt + ", retrying: ", e);
			} catch (IOException e) {
				if (!retryIfNotExists || attempt >= maxAttempts) {
					LOG.debug("getFileStatus - couldn't complete request.", e);
					throw e;
				} else
					LOG.debug("caught exception in attempt " + attempt + ", retrying: ", e);
			} catch (Throwable e) {
				LOG.warn("Couldn't complete getFileStatus.", e);
				throw e;
			}
			try {
				Thread.sleep(retrySleep);
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
			return new FileStatus[] { fileStat };
		}

		String strPath = path.toUri().getPath();

		List<SFTPv3DirectoryEntry> sftpFiles = getClient().ls(strPath);
		ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>(
				sftpFiles.size());
		for (SFTPv3DirectoryEntry sftpFile : sftpFiles) {
			String filename = sftpFile.filename;
			if (!"..".equals(filename) && !".".equals(filename)){
				Path file = new Path(path,
						filename).makeQualified(this);
				fileStats.add(getFileStatus(sftpFile.attributes, file));
			}
		}
		return fileStats.toArray(new FileStatus[0]);
	}

	@Override
	public boolean exists(Path file) throws IOException {
		return exists(file, true);
	}

	private boolean exists(Path file, boolean retryIfNotExists) throws IOException {
		try {
			return getFileStatus(file, retryIfNotExists) != null;
		} catch (FileNotFoundException e) {
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