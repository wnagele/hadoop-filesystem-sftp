package org.apache.hadoop.fs.sftp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem.SFTPv3ClientWrapper;

import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileHandle;

public class SFTPInputStream extends FSInputStream {
	public static final Log LOG = LogFactory.getLog(SFTPInputStream.class);
	
	public static final int MAX_READ_SIZE = 32768;
	  
	private SFTPv3FileHandle handle;
	private FileSystem.Statistics stats;
	private SFTPv3Client client;
	private long pos = 0;
	

	public SFTPInputStream(SFTPv3FileHandle handle, FileSystem.Statistics stats) {
		this.handle = handle;
		this.stats = stats;
		this.client = handle.getClient();
		LOG.info("created input stream");
	}

	@Override
	public void close() throws IOException {
		LOG.info("closing input stream");
		super.close();
		client.closeFile(handle);
		client.close();
		if (client instanceof SFTPv3ClientWrapper) {
			LOG.info("Closing specific connection");
			((SFTPv3ClientWrapper)client).getConnection().close();
		}
	}

	@Override
	public synchronized int read() throws IOException {
		byte[] buf = new byte[1];
        return read(buf, 0, 1) == -1 ? -1 : buf[0] & 0xff;
	}

	@Override
	public synchronized int read(byte[] buf, int off, int len) throws IOException {
		return read(pos, buf, off, len);
	}

	@Override
	public int read(long position, byte[] buf, int off, int len) throws IOException {
		return readInternal(position, buf, off, len);
	}
	
	private int readInternal(long position, byte[] buf, int off, int len) throws IOException {
		int fixedLen = len < MAX_READ_SIZE ? len : MAX_READ_SIZE;	//SFTP client only supports reads of up to 32768
		LOG.debug("reading stream internal - pos=" + pos + " position="+ position + " off=" + off + " len=" + len + " fixedlen=" + fixedLen);
		if (len == 0)
			return 0;
        int read = client.read(handle, position, buf, off, fixedLen);
		LOG.debug("read from stream internal - read=" + read);
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