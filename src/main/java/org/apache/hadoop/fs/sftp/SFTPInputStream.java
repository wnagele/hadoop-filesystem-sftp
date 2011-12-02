package org.apache.hadoop.fs.sftp;

import java.io.IOException;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import ch.ethz.ssh2.SFTPv3FileHandle;

public class SFTPInputStream extends FSInputStream {
	private SFTPv3FileHandle handle;
	private FileSystem.Statistics stats;
	private long pos = 0;

	public SFTPInputStream(SFTPv3FileHandle handle, FileSystem.Statistics stats) {
		this.handle = handle;
		this.stats = stats;
	}

	@Override
	public void close() throws IOException {
		super.close();
		handle.getClient().closeFile(handle);
	}

	@Override
	public synchronized int read() throws IOException {
		byte[] buf = new byte[1];
        int read = handle.getClient().read(handle, pos, buf, 0, 1);
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
        int read = handle.getClient().read(handle, position, buf, off, len);
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
	public boolean markSupported() {
		return false;
	}

	@Override
	public void mark(int readLimit) {}

	@Override
	public void reset() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seek(long targetPos) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		throw new UnsupportedOperationException();
	}
}