package org.apache.hadoop.fs.sftp;

import java.io.IOException;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileHandle;

public class SFTPInputStream extends FSInputStream {
	private SFTPv3FileHandle handle;
	private FileSystem.Statistics stats;
	private SFTPv3Client client;
	private long pos = 0;

	public SFTPInputStream(SFTPv3FileHandle handle, FileSystem.Statistics stats) {
		this.handle = handle;
		this.stats = stats;
		this.client = handle.getClient();
	}

	@Override
	public void close() throws IOException {
		super.close();
		client.closeFile(handle);
	}

	@Override
	public synchronized int read() throws IOException {
		byte[] buf = new byte[1];
        int read = client.read(handle, pos, buf, 0, 1);
        if (read == -1)
        	return -1;

        pos++;
		if (stats != null)
			stats.incrementBytesRead(1);
        return buf[0];
	}

	@Override
	public synchronized int read(byte[] buf, int off, int len) throws IOException {
		return read(pos, buf, off, len);
	}

	@Override
	public int read(long position, byte[] buf, int off, int len) throws IOException {
        int read = client.read(handle, position, buf, off, len);
        if (read == -1)
        	return -1;

		pos += read;
		if (stats != null)
			stats.incrementBytesRead(read);
		return read;
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void seek(long targetPos) throws IOException {
		pos = targetPos;
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		throw new UnsupportedOperationException();
	}
}