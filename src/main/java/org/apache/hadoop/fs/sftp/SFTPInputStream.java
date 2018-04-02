package org.apache.hadoop.fs.sftp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPException;

public class SFTPInputStream extends FSInputStream {
	public static final Log LOG = LogFactory.getLog(SFTPInputStream.class);
	
	public static final int MAX_READ_SIZE = 32768;
	  
	private RemoteFile handle;
	private FileSystem.Statistics stats;
	private long pos = 0;
	

	public SFTPInputStream(RemoteFile handle, FileSystem.Statistics stats) {
		this.handle = handle;
		this.stats = stats;
		LOG.info("created input stream");
	}

	@Override
	public void close() throws IOException {
		LOG.info("closing input stream");
		super.close();
		try {
			handle.close();
		} catch (IOException e) {
			LOG.warn("SFTP file handle not closed properly: ", e);
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
		//int fixedLen = len < MAX_READ_SIZE ? len : MAX_READ_SIZE;	//SFTP client only supports reads of up to 32768
		LOG.debug("reading stream internal - pos=" + pos + " position="+ position + " off=" + off + " len=" + len);// + " fixedlen=" + fixedLen);
		if (len == 0)
			return 0;
		try {
			int read = handle.read(position, buf, off, len);
			LOG.debug("read from stream internal - read=" + read);
	        if (read == -1)
	        	return -1;
			pos += read;
			if (stats != null)
				stats.incrementBytesRead(read);
	        return read;
		} catch(SFTPException e) {
			LOG.error("getStatusCode=" + e.getStatusCode());
			LOG.error("getDisconnectReason=" + e.getDisconnectReason());
			throw e;
		}

	}
	
	

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void seek(long targetPos) throws IOException {
		//LOG.error("seek=" + targetPos);
		pos = targetPos;
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		throw new UnsupportedOperationException();
	}
}