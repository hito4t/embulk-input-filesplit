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
		
		try (InputStream in = createInput("12345\n67\n89", 1, 7)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 1, 7)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
		}
		
		try (InputStream in = createInput("12345\n67\n89", 1, 10)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 1, 10)) {
			assertEquals(5, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
			assertEquals('8', buffer[3]);
			assertEquals('9', buffer[4]);
		}
		
		try (InputStream in = createInput("12345\n67\n89", 10, 11)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 10, 11)) {
			assertEquals(-1, in.read(buffer));
		}
		
		// \nの直後まで
		try (InputStream in = createInput("12345\n67\n89", 4, 9)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 4, 9)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
			assertEquals(-1, in.read(buffer));
		}
		try (InputStream in = createInput("12345\n67\n89", 4, 9)) {
			assertEquals(3, in.read(buffer, 0, 3));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
			assertEquals(-1, in.read(buffer));
		}
		// \nの直後から
		try (InputStream in = createInput("12345\n67\n89", 9, 11)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 9, 11)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}
		
		// \nの直前まで
		try (InputStream in = createInput("12345\n67\n89", 4, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 4, 8)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
		}
		
		// \nの直前から
		try (InputStream in = createInput("12345\n67\n89", 8, 11)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n89", 8, 11)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}

		// \nで終わる（最後まで）
		try (InputStream in = createInput("12345\n67\n", 5, 9)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n", 5, 9)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
		}

		// \nで終わる（直前まで）
		try (InputStream in = createInput("12345\n67\n", 5, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\n67\n", 5, 8)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\n', buffer[2]);
		}
	}
	
	
	@Test
	public void testCRLF() throws IOException {
		try (InputStream in = createInput("12345\r\n67\r\n89", 0, 4)) {
			assertEquals('1', in.read());
			assertEquals('2', in.read());
			assertEquals('3', in.read());
			assertEquals('4', in.read());
			assertEquals('5', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 0, 4)) {
			assertEquals(7, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
			assertEquals('4', buffer[3]);
			assertEquals('5', buffer[4]);
			assertEquals('\r', buffer[5]);
			assertEquals('\n', buffer[6]);
		}
		
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 4)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 4)) {
			assertEquals(-1, in.read(buffer));
		}
		
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 8)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
		}
		
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 12)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 1, 12)) {
			assertEquals(6, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
			assertEquals('8', buffer[4]);
			assertEquals('9', buffer[5]);
		}
		
		try (InputStream in = createInput("12345\r\n67\r\n89", 12, 13)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 12, 13)) {
			assertEquals(-1, in.read(buffer));
		}
		
		// \nの直後まで
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 11)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 11)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
			assertEquals(-1, in.read(buffer));
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 11)) {
			assertEquals(4, in.read(buffer, 0, 4));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
			assertEquals(-1, in.read(buffer));
		}
		// \nの直後から
		try (InputStream in = createInput("12345\r\n67\r\n89", 11, 13)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 11, 13)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}
		
		// \rと\nの間まで
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 10)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 10)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
		}
		
		// \rと\nの間から
		try (InputStream in = createInput("12345\r\n67\r\n89", 10, 13)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 10, 13)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}
		
		// \rの直前まで
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 9)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 4, 9)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
		}
		
		// \rの直前から
		try (InputStream in = createInput("12345\r\n67\r\n89", 9, 13)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n89", 9, 13)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}

		// \r\nで終わる（最後まで）
		try (InputStream in = createInput("12345\r\n67\r\n", 5, 11)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n", 5, 11)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
		}

		// \r\nで終わる（直前まで）
		try (InputStream in = createInput("12345\r\n67\r\n", 5, 10)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('\n', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r\n67\r\n", 5, 10)) {
			assertEquals(4, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('\n', buffer[3]);
		}
	}
	
	@Test
	public void testCR() throws IOException {
		try (InputStream in = createInput("12345\r67\r89", 0, 4)) {
			assertEquals('1', in.read());
			assertEquals('2', in.read());
			assertEquals('3', in.read());
			assertEquals('4', in.read());
			assertEquals('5', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 0, 4)) {
			assertEquals(6, in.read(buffer));
			assertEquals('1', buffer[0]);
			assertEquals('2', buffer[1]);
			assertEquals('3', buffer[2]);
			assertEquals('4', buffer[3]);
			assertEquals('5', buffer[4]);
			assertEquals('\r', buffer[5]);
		}
		
		try (InputStream in = createInput("12345\r67\r89", 1, 4)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 1, 4)) {
			assertEquals(-1, in.read(buffer));
		}
		
		try (InputStream in = createInput("12345\r67\r89", 1, 7)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 1, 7)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
		}
		
		try (InputStream in = createInput("12345\r67\r89", 1, 10)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 1, 10)) {
			assertEquals(5, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals('8', buffer[3]);
			assertEquals('9', buffer[4]);
		}
		
		try (InputStream in = createInput("12345\r67\r89", 10, 11)) {
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 10, 11)) {
			assertEquals(-1, in.read(buffer));
		}
		
		// \rの直後まで
		try (InputStream in = createInput("12345\r67\r89", 4, 9)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 4, 9)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals(-1, in.read(buffer));
		}
		try (InputStream in = createInput("12345\r67\r89", 4, 9)) {
			assertEquals(3, in.read(buffer, 0, 3));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
			assertEquals(-1, in.read(buffer));
		}
		// \rの直後から
		try (InputStream in = createInput("12345\r67\r89", 9, 11)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 9, 11)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}
		
		// \rの直前まで
		try (InputStream in = createInput("12345\r67\r89", 4, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 4, 8)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
		}
		
		// \rの直前から
		try (InputStream in = createInput("12345\r67\r89", 8, 11)) {
			assertEquals('8', in.read());
			assertEquals('9', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r89", 8, 11)) {
			assertEquals(2, in.read(buffer));
			assertEquals('8', buffer[0]);
			assertEquals('9', buffer[1]);
		}

		// \rで終わる（最後まで）
		try (InputStream in = createInput("12345\r67\r", 5, 9)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r", 5, 9)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
		}

		// \rで終わる（直前まで）
		try (InputStream in = createInput("12345\r67\r", 5, 8)) {
			assertEquals('6', in.read());
			assertEquals('7', in.read());
			assertEquals('\r', in.read());
			assertEquals(-1, in.read());
		}
		try (InputStream in = createInput("12345\r67\r", 5, 8)) {
			assertEquals(3, in.read(buffer));
			assertEquals('6', buffer[0]);
			assertEquals('7', buffer[1]);
			assertEquals('\r', buffer[2]);
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
