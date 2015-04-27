package org.embulk.input.filesplit;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.embulk.input.filesplit.LocalFileSplitInputPlugin.LocalFileSplitInput.FileSplitProvider;
import org.junit.Test;

public class LocalFileSplitInputTest {

	@Test
	public void testHeader() throws Exception
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 0, 20)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 0, 10)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 1, 2)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 1, 20)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 1, 40)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("1,aaaaa,12345", reader.readLine());
			assertEquals("2,bbb,67890", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-header.csv", 20, 40)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals("2,bbb,67890", reader.readLine());
			assertEquals(null, reader.readLine());
		}
	}

	@Test
	public void testOnlyHeader() throws Exception
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-only-header.csv", 0, 10)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(open("/data/test-only-header.csv", 1, 10)))) {
			assertEquals("id,name,value", reader.readLine());
			assertEquals(null, reader.readLine());
		}
	}

	private InputStream open(String name, int start, int end) throws IOException, URISyntaxException
	{
		File path = new File(getClass().getResource(name).toURI());
		try (FileSplitProvider provider = new FileSplitProvider(new PartialFile(path.getAbsolutePath(), start, end), true)) {
			return provider.openNext();
		}
	}

}
