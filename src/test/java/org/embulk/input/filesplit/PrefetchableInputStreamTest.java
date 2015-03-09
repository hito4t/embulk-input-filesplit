package org.embulk.input.filesplit;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class PrefetchableInputStreamTest {
	private byte[] buffer = new byte[8];
	
	@Test
	public void testRead() throws IOException {
		try (PrefetchableInputStream in = createInput("")) {
			assertEquals(-1, in.prefetch());
			assertEquals(-1, in.prefetch());
			assertEquals(-1, in.read());
			assertEquals(-1, in.prefetch());
		}
		
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.prefetch());
			assertEquals('1', in.prefetch());
			assertEquals('1', in.read());
			assertEquals('2', in.prefetch());
			assertEquals('2', in.read());
			assertEquals('3', in.prefetch());
			assertEquals('3', in.read());
			assertEquals(-1, in.prefetch());
			assertEquals(-1, in.read());
		}
	}
	
	@Test
	public void testSkip() throws IOException {
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals(2, in.skip(2));
			assertEquals('3', in.read());
		}
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.prefetch());
			assertEquals(2, in.skip(2));
			assertEquals('3', in.read());
		}
		
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals(3, in.skip(4));
			assertEquals(-1, in.read());
		}
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.prefetch());
			assertEquals(3, in.skip(4));
			assertEquals(-1, in.read());
		}
	}
	
	@Test
	public void testReadBytes() throws IOException {
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals(3, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
			
			assertEquals(-1, in.read(buffer));
			assertEquals(-1, in.prefetch());
			assertEquals(-1, in.read(buffer));
		}
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.prefetch());
			assertEquals(3, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
		}
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.prefetch());
			assertEquals(1, in.read(buffer, 0, 1));
			assertEquals('1', buffer[0]);
		}
		try (PrefetchableInputStream in = createInput("123")) {
			assertEquals('1', in.read());
			assertEquals('2', in.read());
			assertEquals('3', in.prefetch());
			assertEquals(1, in.read(buffer));
			assertEquals('3', buffer[0]);
		}
	}

	private PrefetchableInputStream createInput(String s) {
		try {
			return new PrefetchableInputStream(new ByteArrayInputStream(s.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
