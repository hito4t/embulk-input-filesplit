package org.embulk.input.filesplit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;


public class LocalFileSplitInputPlugin
        implements FileInputPlugin
{
	public interface PluginTask
            extends Task
    {
        @Config("path")
        public String getPath();

        public List<PartialFile> getFiles();
        public void setFiles(List<PartialFile> files);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        long size = new File(task.getPath()).length();
        int division = Runtime.getRuntime().availableProcessors() * 2;
        List<PartialFile> files = new ArrayList<PartialFile>();
        for (int i = 0; i < division; i++) {
        	long start = BigInteger.valueOf(size).multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(division)).longValue();
        	long end = BigInteger.valueOf(size).multiply(BigInteger.valueOf(i + 1)).divide(BigInteger.valueOf(division)).longValue();
        	if (start < end) {
        		files.add(new PartialFile(task.getPath(), start, end));
        	}
        }
        
        task.setFiles(files);

        int taskCount = task.getFiles().size();
        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<CommitReport> successCommitReports)
    { }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new LocalFileSplitInput(task, taskIndex);
    }

    public static class LocalFileSplitInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        private static class FileSplitProvider
                implements InputStreamFileInput.Provider
        {
            private final PartialFile file;
            private boolean opened = false;

            public FileSplitProvider(PartialFile file)
            {
                this.file = file;
            }

            @Override
            public InputStream openNext() throws IOException
            {
                if (opened) {
                    return null;
                }
                opened = true;
                return new PartialFileInputStream(new FileInputStream(file.getPath()), file.getStart(), file.getEnd());
            }

            @Override
            public void close() { }
        }

        public LocalFileSplitInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new FileSplitProvider(task.getFiles().get(taskIndex)));
        }

        @Override
        public void abort() { }

        @Override
        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }
    }
}
