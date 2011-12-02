package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;

public class SFTPOutputStream extends OutputStream {
	private SFTPv3FileHandle handle;
	private FileSystem.Statistics stats;
	private SFTPv3Client client;
	private long pos;

	public SFTPOutputStream(SFTPv3FileHandle handle, FileSystem.Statistics stats) throws IOException {
		this.handle = handle;
		this.stats = stats;
		this.client = handle.getClient();

		SFTPv3FileAttributes attrs = client.fstat(handle);
		pos += attrs.size;
	}

	@Override
	public void close() throws IOException {
		super.close();
		client.closeFile(handle);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		client.write(handle, pos, b, off, len);
		pos += len;
		if (stats != null)
			stats.incrementBytesWritten(len);
	}

	@Override
	public void write(int b) throws IOException {
		client.write(handle, pos, new byte[] { (byte)b }, 0, 1);
        pos++;
		if (stats != null)
			stats.incrementBytesWritten(1);
	}
}