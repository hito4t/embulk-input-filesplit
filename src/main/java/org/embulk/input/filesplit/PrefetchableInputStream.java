/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.input.filesplit;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;


public class PrefetchableInputStream extends InputStream {
	
	private final InputStream original;
	private Integer next;
	
	public PrefetchableInputStream(InputStream original) {
		this.original = new BufferedInputStream(original);
	}
	
	public int prefetch() throws IOException {
		if (next == null) {
			next = original.read();
		}
		return next;
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (next == null) {
			return original.read(b, off, len);
		}
		
		if (next == -1) {
			return -1;
		}
		
		b[off] = next.byteValue();
		next = null;
		if (len == 1) {
			return 1;
		}
		
		int read = original.read(b, off + 1, len - 1);
		if (read < 0) {
			return 1;
		}
		return 1 + read;
	}

	@Override
	public int read() throws IOException {
		if (next == null) {
			return original.read();
		}

		int read = next;
		next = null;
		return read;
	}
	
	@Override
	public long skip(long n) throws IOException {
		if (next == null) {
			return original.skip(n);
		}
		
		next = null;
		return 1 + original.skip(n - 1);
	}
	
	@Override
	public int available() throws IOException {
		return 0;
	}
	
	@Override
	public void close() throws IOException {
		original.close();
	}

}
