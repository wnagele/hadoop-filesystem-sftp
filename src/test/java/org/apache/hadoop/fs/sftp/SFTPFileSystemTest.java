package org.apache.hadoop.fs.sftp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class SFTPFileSystemTest {
	private Configuration conf;

	@Before
	public void init() throws Exception {
		conf = new Configuration();
	}

	@Test
	public void bufferSizeNotSet() throws Exception {
		conf.setInt("io.file.buffer.size", -1);
		initDummyFileSystem();
		assertEquals(32768, conf.getInt("io.file.buffer.size", -1));
	}

	@Test
	public void bufferSizeAboveMaximum() throws Exception {
		conf.setInt("io.file.buffer.size", 100000);
		initDummyFileSystem();
		assertEquals(32768, conf.getInt("io.file.buffer.size", -1));
	}

	@Test
	public void bufferSizeBelowMaximum() throws Exception {
		conf.setInt("io.file.buffer.size", 5000);
		initDummyFileSystem();
		assertEquals(5000, conf.getInt("io.file.buffer.size", -1));
	}

	private SFTPFileSystem initDummyFileSystem() throws IOException {
		SFTPFileSystem sftpFs = new SFTPFileSystem() {
			@Override
			protected void connect() throws IOException {}
		};
		sftpFs.initialize(URI.create("sftp://host"), conf);
		return sftpFs;
	}
}