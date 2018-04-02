package org.apache.hadoop.fs.sftp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode.Type;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.Response.StatusCode;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyFormat;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.userauth.keyprovider.KeyProviderUtil;
import net.schmizz.sshj.userauth.password.PasswordUtils;
import net.schmizz.sshj.xfer.FilePermission;

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
	private SFTPClient client;
	private SSHClient connection;

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
		if (connection == null || !connection.isConnected()) {
			connection = createConnection();
			LOG.info("Creating client");
			client = connection.newSFTPClient();
		}
	}
	
	protected SFTPClient getClient() throws IOException {
		connect();
		return client;		
	}
	
	protected SSHClient createConnection() throws IOException {
		LOG.info("Creating connection");
		String host = conf.get(PARAM_HOST);
		int port = conf.getInt(PARAM_PORT, DEFAULT_PORT);
		String key = conf.get(PARAM_KEY_FILE, DEFAULT_KEY_FILE);
		String keyPassword = conf.get(PARAM_KEY_PASSWORD);
		String user = conf.get(PARAM_USER);
		final String password = conf.get(PARAM_PASSWORD);
		
		SSHClient connection = new SSHClient();

		connection.addHostKeyVerifier(new HostKeyVerifier() {
			
			@Override
			public boolean verify(String hostname, int port, PublicKey key) {
				return true;
			}
		});
		LOG.info("Connecting SSH client to " + host);
		connection.connect(host, port);

		if (password != null) {
			LOG.info("Authenticating STFP using password.");
			connection.authPassword(user, password);
		} else {
			LOG.info("Authenticating STFP using private key.");
			final File loc = new File(key);
	        final KeyFormat format = KeyProviderUtil.detectKeyFileFormat(loc);
			KeyProvider keyProvider = connection.loadKeys(key, PasswordUtils.createOneOff(keyPassword.toCharArray()));
			connection.authPublickey(user, keyProvider);
		}
		
		return connection;		
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
		RemoteFile handle = getClient().open(path);
		SFTPInputStream is = new SFTPInputStream(handle, statistics);
		return is;
	}
	
	private Set<OpenMode> getCreateOpenMode(boolean overwrite) {
		Set<OpenMode> set = EnumSet.of(OpenMode.CREAT, OpenMode.WRITE);
		if (overwrite)
			set.add(OpenMode.TRUNC);
		else
			set.add(OpenMode.EXCL);
		return set;
	}

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		return createInternal(file, permission, getCreateOpenMode(overwrite));
	}

	@Override
	public FSDataOutputStream append(Path file, int bufferSize,
			Progressable progress) throws IOException {
		return createInternal(file, null, EnumSet.of(OpenMode.WRITE, OpenMode.APPEND));
	}

	protected FSDataOutputStream createInternal(Path file,
			FsPermission permission, Set<OpenMode> openMode)
			throws IOException {
//		if (exists(file, false))
//			throw new IOException("File " + file + " exists");

		LOG.info("Opening file for write: " + file.toString());
		
		Path parent = file.getParent();
		if (!exists(parent, true))
			mkdirs(parent);

		FileAttributes attrs = null;
		if (permission != null) {
			attrs = new FileAttributes.Builder().withPermissions(new Short(permission.toShort()).intValue()).build();
		}

		String path = file.toUri().getPath();
		RemoteFile handle = getClient().open(path, openMode);
		SFTPOutputStream os = new SFTPOutputStream(handle, statistics);
		return new FSDataOutputStream(new BufferedOutputStream(os), statistics);
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission)
			throws IOException {
		LOG.info("creating dir: " + file.toString());
		if (!exists(file, false)) {
			getClient().mkdirs(file.toUri().getPath());
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
				getClient().rename(oldPath, newPath);	
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
		FileAttributes attrs = new FileAttributes.Builder()
			.withAtimeMtime(atime / 1000, mtime / 1000).build();
		getClient().setattr(path, attrs);
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
				FileAttributes attrs = getClient().stat(path);
				LOG.info("getFileStatus - getting file status from attributes" + file.toUri().toString() + " len=" + attrs.getSize());
				FileStatus status = getFileStatus(attrs, file);
				return status;
						
			} catch (SFTPException e) {
				if (!retryIfNotExists || attempt >= maxAttempts) {
					if (e.getStatusCode() == StatusCode.NO_SUCH_FILE || e.getStatusCode() == StatusCode.NO_SUCH_PATH) {
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

	private FileStatus getFileStatus(FileAttributes attrs, Path file)
			throws IOException {
		long length = attrs.getSize();
		boolean isDir = attrs.getType() == Type.DIRECTORY;
		long modTime = attrs.getMtime() * 1000;
		long accessTime = attrs.getAtime() * 1000;
		FsPermission permission = new FsPermission(new Integer(FilePermission.toMask(attrs.getPermissions())).shortValue());
		String user = Integer.toString(attrs.getUID()); 
		String group  = Integer.toString(attrs.getGID()); 
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

		List<RemoteResourceInfo> sftpFiles = getClient().ls(strPath);
		ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>(
				sftpFiles.size());
		for (RemoteResourceInfo sftpFile : sftpFiles) {
			String filename = sftpFile.getPath();
			if (!"..".equals(filename) && !".".equals(filename)){
				Path file = new Path(filename).makeQualified(this);
				fileStats.add(getFileStatus(sftpFile.getAttributes(), file));
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
		//LOG.info("getFileBlockLocations - start = " + start + " , len = " + len);
		//LOG.info("getFileBlockLocations: path=" + file.getPath().toUri().toString() + " len=" + file.getLen() + " blocksize=" + file.getBlockSize() + " isdir=" + file.isDir());
		return super.getFileBlockLocations(file, start, len);
	}
	
}