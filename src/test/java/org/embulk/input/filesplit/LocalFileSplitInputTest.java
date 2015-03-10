/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.input.filesplit;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.embulk.input.filesplit.LocalFileSplitInputPlugin.LocalFileSplitInput.FileSplitProvider;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LocalFileSplitInputTest {
	
	@Test
	public void testHeader() throws Exception
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(0, 20)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(0, 10)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(1, 2)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}
		
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(1, 20)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals(null, reader.readLine());
		}
		
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(1, 40)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals("2,bbb,67890", reader.readLine());
			assertEquals(null, reader.readLine());
		}
		
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(20, 40)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("2,bbb,67890", reader.readLine());
			assertEquals(null, reader.readLine());
		}
	}
	
	private InputStream open(int start, int end) throws IOException, URISyntaxException 
	{
		File path = new File(getClass().getResource("/data/test-header.csv").toURI());
		try (FileSplitProvider provider = new FileSplitProvider(new PartialFile(path.getAbsolutePath(), start, end), true)) {
			return provider.openNext();
		}
	}
	
}
