package org.embulk.input.filesplit;

import java.io.IOException;
import java.io.InputStream;


public class PartialFileInputStream extends InputStream {
	
	private final PrefetchableInputStream original;
	private long start;
	private long end;
	private long current;
	private boolean eof;
	
	public PartialFileInputStream(InputStream original, long start, long end) {
		this.original = new PrefetchableInputStream(original);
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
		
		int read = original.read(b, off, len);
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
				
				if (b[off + i] == '\r') {
					int next = (i < read ? b[off + i + 1] : original.prefetch());
					if (next != '\n') {
						eof = true;
						return i + 1;
					}
				}
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
		
		int read = original.read();
		current++;
		
		if (read < 0) {
			eof = true;
			return -1;
		}
		
		if (current >= end) {
			if (read == '\n' || read == '\r' && original.prefetch() != '\n') {
				eof = true;
			}
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
				
				if (c == '\n' || c == '\r' && original.prefetch() != '\n') {
					break;
				}
			}
		}
		
		if (start >= end) {
			eof = true;
		}
	}
}
