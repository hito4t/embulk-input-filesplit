package org.embulk.input.filesplit;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.embulk.spi.InputPlugin;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalFileSplitInputPluginTest {

    private static EmbulkPluginTester tester = new EmbulkPluginTester(InputPlugin.class, "filesplit", LocalFileSplitInputPlugin.class);

    @Test
    public void test() throws Exception
    {
        run("/yml/test.yml", "/data/test-semicolon.csv");
    }

    @Test
    public void testTasks() throws Exception
    {
        run("/yml/test-tasks.yml", "/data/test-semicolon.csv");
    }

    @Test
    public void testHeader() throws Exception
    {
        run("/yml/test-header.yml", "/data/test-semicolon.csv");
    }

    @Test
    public void testOnlyHeader() throws Exception
    {
        run("/yml/test-only-header.yml", "/data/empty.csv");
    }

    @Test
    public void testPathPrefixDirectory() throws Exception
    {
        run("/yml/test-path_prefix-directory.yml", "/data/test.csv");
    }

    @Test
    public void testPathPrefixFiles() throws Exception
    {
        run("/yml/test-path_prefix-files.yml", "/data/test.csv");
    }

    @Test
    public void testError1() throws Exception
    {
        try {
            run("/yml/test-error1.yml", "/data/empty.csv");
            fail("IllegalArgumentException expected.");
        } catch (IllegalArgumentException e) {
            System.err.println(e);
        }
    }

    @Test
    public void testError2() throws Exception
    {
        try {
            run("/yml/test-error2.yml", "/data/empty.csv");
            fail("IllegalArgumentException expected.");
        } catch (IllegalArgumentException e) {
            System.err.println(e);
        }
    }

    private void run(String ymlPath, String expectedName) throws Exception
    {
        List<String> expected = readAll(expectedName);
        Collections.sort(expected);

        File file = prepare();
        tester.run(ymlPath);

        List<String> actual= readAll(file);
        Collections.sort(actual);

        assertEquals(expected, actual);
    }

    private File prepare() throws URISyntaxException
    {
        File file = new File(new File(getClass().getResource("/resource.txt").toURI()).getParentFile(), "temp");
        file.mkdir();
        for (File child : file.listFiles()) {
            child.delete();
        }
        return file;
    }

    private List<String> readAll(String name) throws IOException, URISyntaxException
    {
        return readAll(new File(getClass().getResource(name).toURI()));
    }

    private List<String> readAll(File file) throws IOException
    {
        if (file.isFile()) {
            FileSystem fs = FileSystems.getDefault();
            Charset charset = Charset.forName("UTF-8");
            return Files.readAllLines(fs.getPath(file.getAbsolutePath()), charset);
        }

        if (file.isDirectory()) {
            List<String> lines = new ArrayList<String>();
            for (File child : file.listFiles()) {
                lines.addAll(readAll(child));
            }
            return lines;
        }

        return Collections.emptyList();
    }

}
