package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * Based on implementation posted on Hadoop JIRA
 * @see https://issues.apache.org/jira/browse/HADOOP-5732
 * @author wnagele
 */
public class SFTPFileSystem extends FileSystem {
	private final int DEFAULT_PORT = 22;
	private final String DEFAULT_KEY_FILE = "${user.home}/.ssh/id_rsa";
	private final String DEFAULT_KNOWNHOSTS_FILE = "${user.home}/.ssh/known_hosts";

	private final String PARAM_HOST = "fs.sftp.host";
	private final String PARAM_PORT = "fs.sftp.port";
	private final String PARAM_USER = "fs.sftp.user";
	private final String PARAM_PASSWORD = "fs.sftp.password";
	private final String PARAM_KEY_FILE = "fs.sftp.key.file";
	private final String PARAM_KEY_PASSWORD = "fs.sftp.key.password";
	private final String PARAM_KNOWNHOSTS = "fs.sftp.knownhosts";

	private Configuration conf;
	private URI uri;
	private ChannelSftp c;
	private Session session;

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		this.uri = uri;
		this.conf = conf;

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

		connect();
	}

	private void connect() throws IOException {
		if (c == null || !c.isConnected()) {
			String host = conf.get(PARAM_HOST);
			int port = conf.getInt(PARAM_PORT, DEFAULT_PORT);
			String key = conf.get(PARAM_KEY_FILE, DEFAULT_KEY_FILE);
			String keyPassword = conf.get(PARAM_KEY_PASSWORD);
			String user = conf.get(PARAM_USER);
			String password = conf.get(PARAM_PASSWORD);
			String knownHostsFile = conf.get(PARAM_KNOWNHOSTS, DEFAULT_KNOWNHOSTS_FILE);

			JSch jsch = new JSch();
			try {
				jsch.setKnownHosts(knownHostsFile);

				if (key != null) {
					if (keyPassword != null)
						jsch.addIdentity(key, keyPassword);
					else
						jsch.addIdentity(key);
				}

				session = jsch.getSession(user, host, port);

				MyUserInfo ui = new MyUserInfo();
				ui.setPassword(password);
				session.setUserInfo(ui);

				session.connect();

				Channel channel = session.openChannel("sftp");
				channel.connect();
				c = (ChannelSftp) channel;
			} catch (JSchException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (c != null && c.isConnected())
			c.disconnect();
		if (session != null && session.isConnected())
			session.disconnect();
	}

	@Override
	public FSDataInputStream open(Path file, int bufferSize) throws IOException {
		try {
			Path absolute = makeAbsolute(file);

			FileStatus fileStat = getFileStatus(absolute);
			if (fileStat.isDir())
				throw new IOException("Path " + file + " is a directory.");

			Path parent = absolute.getParent();
			c.cd(parent.toUri().getPath());
			InputStream is = c.get(file.getName());
			return new FSDataInputStream(new SFTPInputStream(is, statistics));
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		return createOutputStream(file, permission, overwrite, bufferSize, replication, blockSize, progress, ChannelSftp.OVERWRITE);
	}

	@Override
	public FSDataOutputStream append(Path file, int bufferSize, Progressable progress) throws IOException {
		return createOutputStream(file, null, true, bufferSize, (short)-1, -1, progress, ChannelSftp.APPEND);
	}

	private FSDataOutputStream createOutputStream(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress, int sftpMode) throws IOException {
		try {
			Path absolute = makeAbsolute(file);

			if (exists(file) && !overwrite)
				throw new IOException("File " + file + " exists");

			Path parent = absolute.getParent();
			if (parent == null)
				parent = (parent == null) ? new Path("/").makeQualified(this) : parent;

			if (!exists(parent))
				mkdirs(parent, permission);

			c.cd(parent.toUri().getPath());
			return new FSDataOutputStream(c.put(file.getName(), sftpMode), statistics);
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission) throws IOException {
		try {
			Path absolute = makeAbsolute(file);
			if (!exists(absolute)) {
				Path parent = absolute.getParent();
				if (parent == null || mkdirs(parent, permission)) {
					c.cd(parent.toUri().getPath());
					c.mkdir(absolute.getName());
				}
			}
			return true;
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		try {
			Path srcAbsolute = makeAbsolute(src);
			Path dstAbsolute = makeAbsolute(dst);
			c.rename(srcAbsolute.toUri().getPath(),
			         dstAbsolute.toUri().getPath());
			return true;
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean delete(Path file, boolean recursive) throws IOException {
		try {
			Path absolute = makeAbsolute(file);
			String path = absolute.toUri().getPath();
			if (!getFileStatus(absolute).isDir()) {
				c.rm(path);
				return true;
			}

			FileStatus[] dirEntries = listStatus(absolute);
			if (dirEntries != null && dirEntries.length > 0 && !recursive)
				throw new IOException("Directory: " + file + " is not empty.");

			if (dirEntries != null) {
				for (FileStatus dirEntry : dirEntries)
					delete(new Path(absolute, dirEntry.getPath()), recursive);
			}

			c.rmdir(path);
			return true;
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean delete(Path file) throws IOException {
		return delete(file, false);
	}

	@Override
	public void setTimes(Path file, long mtime, long atime) throws IOException {
		try {
			Path absolute = makeAbsolute(file);
			String path = absolute.toUri().getPath();
			SftpATTRS attrs = c.stat(path);
			attrs.setACMODTIME(new Long(atime / 1000L).intValue(),
			                   new Long(mtime / 1000L).intValue());
			c.setStat(path, attrs);
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	@Override
	public FileStatus getFileStatus(Path file) throws IOException {
		try {
			Path absolute = makeAbsolute(file);

			if (absolute.getParent() == null)
				return new FileStatus(-1, true, -1, -1, -1, new Path("/").makeQualified(this));

			SftpATTRS attrs = c.stat(absolute.toUri().getPath());
			return getFileStatus(attrs, file);
		} catch (SftpException e) {
			throw new IOException(e);
		}
	}

	private FileStatus getFileStatus(SftpATTRS attrs, Path file) throws IOException {
		try {
			if (attrs.isLink()) {
				String linkDst = c.readlink(file.toUri().toString());
				SftpATTRS linkDstAttrs = c.stat(linkDst);
				URI linkUri = file.toUri();
				URI uri = new URI(linkUri.getScheme(),
				                  linkUri.getUserInfo(),
				                  linkUri.getHost(),
				                  linkUri.getPort(),
				                  linkDst,
				                  linkUri.getQuery(),
				                  linkUri.getFragment());
				return getFileStatus(linkDstAttrs, new Path(uri));
			}

			long length = attrs.getSize();
			boolean isDir = attrs.isDir();
			long modTime = new Integer(attrs.getMTime()).longValue() * 1000;
			long accessTime = new Integer(attrs.getATime()).longValue() * 1000;
			FsPermission permission = new FsPermission((short)attrs.getPermissions());
			String user = Integer.toString(attrs.getUId());
			String group = Integer.toString(attrs.getGId());
			return new FileStatus(length, isDir, -1, -1, modTime, accessTime, permission, user, group, file);
		} catch (SftpException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		try {
			FileStatus fileStat = getFileStatus(path);
			if (!fileStat.isDir())
				return new FileStatus[] { fileStat };

			@SuppressWarnings("unchecked")
			Vector<LsEntry> sftpFiles = c.ls(path.toUri().getPath());
			ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>(sftpFiles.size());
			for (LsEntry sftpFile : sftpFiles) {
				String filename = sftpFile.getFilename();
				if (!"..".equals(filename) && !".".equals(filename))
					fileStats.add(getFileStatus(sftpFile.getAttrs(), new Path(path, filename).makeQualified(this)));
			}
			return fileStats.toArray(new FileStatus[0]);
		} catch (SftpException e) {
			throw new IOException(e);
		}
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
	public Path getHomeDirectory() {
		try {
			return new Path(c.pwd()).makeQualified(this);
		} catch (SftpException e) {
			return null;
		}
	}

	private Path makeAbsolute(Path path) throws SftpException {
		if (path.isAbsolute())
			return path;
		return new Path(c.pwd(), path).makeQualified(this);
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public void setWorkingDirectory(Path workDir) {
		try {
			Path absolute = makeAbsolute(workDir);
			c.cd(absolute.toUri().getPath());
		} catch (SftpException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Path getWorkingDirectory() {
		try {
			return new Path(c.pwd());
		} catch (SftpException e) {
			e.printStackTrace();
			return null;
		}
	}

	static class MyUserInfo implements UserInfo {
		private String password = null;

		public void setPassword(String ppassword) {
			this.password = ppassword;
		}

		public String getPassword() {
			return this.password;
		}

		public String getPassphrase() {
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			return false;
		}

		public boolean promptPassword(String arg0) {
			return true;
		}

		public boolean promptYesNo(String arg0) {
			return false;
		}

		public void showMessage(String arg0) {
		}
	}
}
