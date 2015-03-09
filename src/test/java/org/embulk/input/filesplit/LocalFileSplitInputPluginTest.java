package org.embulk.input.filesplit;

import org.embulk.spi.InputPlugin;
import org.junit.Test;

public class LocalFileSplitInputPluginTest {
	
	private EmbulkPluginTester tester = new EmbulkPluginTester(InputPlugin.class, "filesplit", LocalFileSplitInputPlugin.class);
	
	@Test
	public void test1() throws Exception
	{
		tester.run("/yml/test1.yml");
		
	}
	
}
