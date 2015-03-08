/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.input.filesplit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartialFileInputStreamTest {
	private byte[] buffer = new byte[8];
	
	@Test
	public void testEmpty() throws IOException {
		try (InputStream in = createInput("", 0, 0)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("", 0, 0)) {
			assertEquals(-1, in.read(buffer));
		}
	}
	
	@Test
	public void testNoLineBreak() throws IOException {
		try (InputStream in = createInput("12345", 0, 4)) {
			assertEquals('1', in.read());
			assertEquals('2', in.read());
			assertEquals('3', in.read());
			assertEquals('4', in.read());
			assertEquals('5', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345", 0, 4)) {
			assertEquals(5, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
			assertEquals('4', buffer[3]);
			assertEquals('5', buffer[4]);
		}
		
		try (InputStream in = createInput("12345", 1, 4)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345", 1, 4)) {
			assertEquals(-1, in.read(buffer));
		}
	}
	
	@Test
	public void testLF() throws IOException {
		try (InputStream in = createInput("12345\n67\n89", 0, 4)) {
			assertEquals('1', in.read());
			assertEquals('2', in.read());
			assertEquals('3', in.read());
			assertEquals('4', in.read());
			assertEquals('5', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 0, 4)) {
			assertEquals(6, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
			assertEquals('4', buffer[3]);
			assertEquals('5', buffer[4]);
			assertEquals('\n', buffer[5]);
		}
		
		try (InputStream in = createInput("12345\n67\n89", 1, 4)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 1, 4)) {
			assertEquals(-1, in.read(buffer));
		}
		
		try (InputStream in = createInput("12345\n67\n89", 1, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 1, 8)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
		}
		
		try (InputStream in = createInput("12345\n67\n89", 1, 11)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 1, 11)) {
			assertEquals(5, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
			assertEquals('8', buffer[3]);
			assertEquals('9', buffer[4]);
		}
	}
	
	private InputStream createInput(String s, int start, int end) {
		try {
			return new PartialFileInputStream(new ByteArrayInputStream(s.getBytes("UTF-8")), start, end);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
