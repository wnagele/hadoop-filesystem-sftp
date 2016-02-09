package org.apache.hadoop.fs.sftp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.KnownHosts;
import ch.ethz.ssh2.SFTPException;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;
import ch.ethz.ssh2.ServerHostKeyVerifier;
import ch.ethz.ssh2.InteractiveCallback;
import ch.ethz.ssh2.sftp.ErrorCodes;

/**
 * Based on implementation posted on Hadoop JIRA.
 * Using Ganymede SSH lib for improved performance.
 * @see https://issues.apache.org/jira/browse/HADOOP-5732
 * @author wnagele
 */
public class SFTPFileSystem extends FileSystem {
	private final int MAX_BUFFER_SIZE = 32768;
	private final int DEFAULT_PORT = 22;
	private final String DEFAULT_KEY_FILE = "${user.home}/.ssh/id_rsa";
	private final String DEFAULT_KNOWNHOSTS_FILE = "${user.home}/.ssh/known_hosts";

	private final String PARAM_BUFFER_SIZE = "io.file.buffer.size";
	private final String PARAM_HOST = "fs.sftp.host";
	private final String PARAM_PORT = "fs.sftp.port";
	private final String PARAM_USER = "fs.sftp.user";
	private final String PARAM_PASSWORD = "fs.sftp.password";
	private final String PARAM_KEY_FILE = "fs.sftp.key.file";
	private final String PARAM_KEY_PASSWORD = "fs.sftp.key.password";
	private final String PARAM_KNOWNHOSTS = "fs.sftp.knownhosts";

	private Configuration conf;
	private URI uri;
	private SFTPv3Client client;
	private Connection connection;

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		Logger.getLogger("ch.ethz.ssh2").setLevel(Level.OFF);

		this.uri = uri;
		this.conf = conf;

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

	/**
	 * The current implementation of @see KnownHosts does not support
	 * known_hosts entries that use a non-default port.
	 * If we encounter such an entry we wrap it into the known_hosts
	 * format before looking it up.
	 */
	private class PortAwareKnownHosts extends KnownHosts {
		public PortAwareKnownHosts(File knownHosts) throws IOException {
			super(knownHosts);
		}

		public int verifyHostkey(String hostname, int port, String serverHostKeyAlgorithm, byte[] serverHostKey) throws IOException {
			if (port != 22) {
				StringBuffer sb = new StringBuffer();
				sb.append('[');
				sb.append(hostname);
				sb.append("]:");
				sb.append(port);
				hostname = sb.toString();
			}

			return super.verifyHostkey(hostname, serverHostKeyAlgorithm, serverHostKey);
		}
	}

	protected void connect() throws IOException {
		if (client == null || !client.isConnected()) {
			String host = conf.get(PARAM_HOST);
			int port = conf.getInt(PARAM_PORT, DEFAULT_PORT);
			String key = conf.get(PARAM_KEY_FILE, DEFAULT_KEY_FILE);
			String keyPassword = conf.get(PARAM_KEY_PASSWORD);
			String user = conf.get(PARAM_USER);
			final String password = conf.get(PARAM_PASSWORD);
			String knownHostsFile = conf.get(PARAM_KNOWNHOSTS, DEFAULT_KNOWNHOSTS_FILE);

			final PortAwareKnownHosts knownHosts = new PortAwareKnownHosts(new File(knownHostsFile));

			connection = new Connection(host, port);
			connection.connect(new ServerHostKeyVerifier() {
				@Override
				public boolean verifyServerHostKey(String hostname, int port, String serverHostKeyAlgorithm, byte[] serverHostKey) throws Exception {
					if (knownHosts.verifyHostkey(hostname, port, serverHostKeyAlgorithm, serverHostKey) == KnownHosts.HOSTKEY_IS_OK)
						return true;
					throw new IOException("Couldn't verify host key for " + hostname);
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

			client = new SFTPv3Client(connection);
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (client != null && client.isConnected())
			client.close();
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

		String path = file.toUri().getPath();
		SFTPv3FileHandle handle = client.openFileRO(path);
		SFTPInputStream is = new SFTPInputStream(handle, statistics);
		return is;
	}

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		return createInternal(file, permission, overwrite, SFTPv3Client.SSH_FXF_CREAT | SFTPv3Client.SSH_FXF_WRITE | SFTPv3Client.SSH_FXF_TRUNC);
	}

	@Override
	public FSDataOutputStream append(Path file, int bufferSize, Progressable progress) throws IOException {
		return createInternal(file, null, true, SFTPv3Client.SSH_FXF_WRITE | SFTPv3Client.SSH_FXF_APPEND);
	}

	protected FSDataOutputStream createInternal(Path file, FsPermission permission, boolean overwrite, int flags) throws IOException {
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
		SFTPv3FileHandle handle = client.openFile(path, flags, attrs);
		SFTPOutputStream os = new SFTPOutputStream(handle, statistics);
		return new FSDataOutputStream(os, statistics);
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission) throws IOException {
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
		if (file.getParent() == null)
			return new FileStatus(-1, true, -1, -1, -1, new Path("/").makeQualified(this));

		try {
			String path = file.toUri().getPath();
			SFTPv3FileAttributes attrs = client.stat(path);
			return getFileStatus(attrs, file);
		} catch (SFTPException e) {
			if (e.getServerErrorCode() == ErrorCodes.SSH_FX_NO_SUCH_FILE)
				throw new FileNotFoundException(file.toString());
			throw e;
		}
	}

	private FileStatus getFileStatus(SFTPv3FileAttributes attrs, Path file) throws IOException {
		long length = attrs.size;
		boolean isDir = attrs.isDirectory();
		long modTime = new Integer(attrs.mtime).longValue() * 1000L;
		long accessTime = new Integer(attrs.atime).longValue() * 1000L;
		FsPermission permission = new FsPermission(new Integer(attrs.permissions).shortValue());
		String user = Integer.toString(attrs.uid);
		String group = Integer.toString(attrs.gid);
		return new FileStatus(length, isDir, -1, -1, modTime, accessTime, permission, user, group, file);
	}

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		FileStatus fileStat = getFileStatus(path);
		if (!fileStat.isDir())
			return new FileStatus[] { fileStat };

		List<SFTPv3DirectoryEntry> sftpFiles = client.ls(path.toUri().getPath());
		ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>(sftpFiles.size());
		for (SFTPv3DirectoryEntry sftpFile : sftpFiles) {
			String filename = sftpFile.filename;
			if (!"..".equals(filename) && !".".equals(filename))
				fileStats.add(getFileStatus(sftpFile.attributes, new Path(path, filename).makeQualified(this)));
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
	public void setWorkingDirectory(Path workDir) {}

	@Override
	public Path getWorkingDirectory() {
		return null;
	}
}