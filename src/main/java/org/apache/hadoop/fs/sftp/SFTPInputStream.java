package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

public class SFTPInputStream extends FSInputStream {
	private InputStream stream;
	private FileSystem.Statistics stats;
	private long pos = 0;

	public SFTPInputStream(InputStream stream, FileSystem.Statistics stats) {
		this.stream = stream;
		this.stats = stats;
	}

	@Override
	public synchronized int read() throws IOException {
		int read = stream.read();
		if (stats != null)
			stats.incrementBytesRead(1);
		pos++;
		return read;
	}

	@Override
	public synchronized int read(byte buf[], int off, int len) throws IOException {
		int read = stream.read(buf, off, len);
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
		return stream.markSupported();
	}

	@Override
	public void mark(int readLimit) {
		stream.mark(readLimit);
	}

	@Override
	public void reset() throws IOException {
		stream.reset();
	}

	@Override
	public int read(long position, byte[] buffer, int offset, int length) throws IOException {
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