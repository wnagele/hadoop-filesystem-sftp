package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import net.schmizz.sshj.sftp.RemoteFile;

public class SFTPOutputStream extends OutputStream {
	  public static final Log LOG = LogFactory.getLog(SFTPOutputStream.class);
	  
	private RemoteFile handle;
	private FileSystem.Statistics stats;
	private long pos;
	private Integer id;

	public SFTPOutputStream(RemoteFile handle, FileSystem.Statistics stats) throws IOException {
		this.handle = handle;
		this.stats = stats;
		LOG.info("created output stream (" + id + ")");
	}

	@Override
	public void close() throws IOException {
		LOG.info("closing output stream (" + id + ")");
		super.close();
		handle.close();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		LOG.debug("writing to stream (" + id + ") - len=" + len);
		handle.write(pos, b, off, len);
		pos += len;
		LOG.debug("writen to stream (" + id + ") - pos=" + pos);
		if (stats != null)
			stats.incrementBytesWritten(len);
	}

	@Override
	public void write(int b) throws IOException {
		LOG.debug("writing int to stream (" + id + ") - b=" + b);
		handle.write(pos, new byte[] { (byte)b }, 0, 1);
        pos++;
		LOG.debug("writen to stream (" + id + ") - pos=" + pos);
		if (stats != null)
			stats.incrementBytesWritten(1);
	}
}