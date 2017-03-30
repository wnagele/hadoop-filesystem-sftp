package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem.SFTPv3ClientWrapper;

import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;

public class SFTPOutputStream extends OutputStream {
	  public static final Log LOG = LogFactory.getLog(SFTPOutputStream.class);
	  
	private SFTPv3FileHandle handle;
	private FileSystem.Statistics stats;
	private SFTPv3Client client;
	private long pos;
	private Integer id;

	public SFTPOutputStream(SFTPv3FileHandle handle, FileSystem.Statistics stats) throws IOException {
		this.handle = handle;
		this.stats = stats;
		this.client = handle.getClient();

		SFTPv3FileAttributes attrs = client.fstat(handle);
		id = attrs.uid;
		pos += attrs.size;
		LOG.info("created output stream (" + id + ")");
	}

	@Override
	public void close() throws IOException {
		LOG.info("closing output stream (" + id + ")");
		super.close();
		client.closeFile(handle);
		client.close();
		if (client instanceof SFTPv3ClientWrapper) {
			LOG.info("closing connection for output stream (" + id + ")");
			((SFTPv3ClientWrapper)client).getConnection().close();
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		LOG.debug("writing to stream (" + id + ") - len=" + len);
		client.write(handle, pos, b, off, len);
		pos += len;
		LOG.debug("writen to stream (" + id + ") - pos=" + pos);
		if (stats != null)
			stats.incrementBytesWritten(len);
	}

	@Override
	public void write(int b) throws IOException {
		LOG.debug("writing int to stream (" + id + ") - b=" + b);
		client.write(handle, pos, new byte[] { (byte)b }, 0, 1);
        pos++;
		LOG.debug("writen to stream (" + id + ") - pos=" + pos);
		if (stats != null)
			stats.incrementBytesWritten(1);
	}
}