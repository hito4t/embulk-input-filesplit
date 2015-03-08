/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.input.filesplit;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;


public class PartialFileInputStream extends InputStream {
	
	private final InputStream original;
	private long start;
	private long end;
	private long current;
	private Integer next;
	private boolean lastIsCR;
	private boolean eof;
	
	public PartialFileInputStream(InputStream original, long start, long end) {
		this.original = new BufferedInputStream(original);
		this.start = start;
		this.end = end;
		current = -1;
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		initializeIfNeeded();

		if (eof) {
			return -1;
		}
		
		int read;
		if (next != null) {
			b[off] = next.byteValue();
			next = null;
			read = 1;
			
			if (len > 1) {
				int temp = original.read(b, off + 1, len - 1);
				if (temp < 0) {
					eof = true;
				} else {
					read += temp;
				}
			}
		} else {
			read = original.read(b, off, len);
		}
		if (read < 0) {
			eof = true;
			return -1;
		}
		
		current += read;
		if (current >= end) {
			for (int i = Math.max((int)(end - 1 - current + read), 0); i < read; i++) {
				if (b[off + i] == '\n') {
					eof = true;
					return i + 1;
				}
				
				if (lastIsCR) {
					eof = true;
					if (i == 0) {
						return -1;
					}
					return i;
				}
				
				lastIsCR = (b[off + i] == '\r');
			}
		}
		
		return read;
	}

	@Override
	public int read() throws IOException {
		initializeIfNeeded();

		if (eof) {
			return -1;
		}
		
		int read;
		if (next != null) {
			read = next;
			next = null;
		} else {
			read = original.read();
		}
		current++;
		
		if (read < 0) {
			eof = true;
			return -1;
		}
		
		if (current >= end) {
			if (read == '\n') {
				eof = true;
				
			} else if (lastIsCR) {
				eof = true;
				return -1;
			}
			
			lastIsCR = (read == '\r');
		}
		
		return read;
	}
	
	@Override
	public long skip(long n) throws IOException {
		throw new IOException("Skip not supported.");
		/*
		long skip = original.skip(n);
		current += skip;
		return skip;
		*/
	}
	
	@Override
	public int available() throws IOException {
		return 0;
	}
	
	@Override
	public void close() throws IOException {
		original.close();
	}
	
	private void initializeIfNeeded() throws IOException {
		if (current >= start) {
			return;
			
		}
		if (start == 0) {
			current = 0;
		} else {
			current = original.skip(--start);
			if (current != start) {
				throw new IOException("Cannot skip.");
			}
			
			int c;
			while ((c = original.read()) >= 0) {
				start++;
				current++;
				
				if (c == '\n') {
					break;
				}
				
				if (lastIsCR) {
					start--;
					current--;
					next = c;
					break;
				} 
				
				lastIsCR = (c == '\r');
			}
		}
		
		lastIsCR = false;
		if (start >= end) {
			eof = true;
		}
	}
}
