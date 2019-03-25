package com.cdc.github;


import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class Reader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomCommitLogReadHandler.class);

    private final WatchService watcher;
    private final Path dir;
    private final WatchKey key;
    private final CommitLogReader commitLogReader;
    private final CustomCommitLogReadHandler commitLogReadHander;

    /**
     * Creates a WatchService and registers the given directory
     */
    public Reader(Map<String, Object> configuration) throws IOException {
        this.dir = Paths.get((String) YamlUtils.select(configuration, "cassandra.cdc_raw_directory"));
        watcher = FileSystems.getDefault().newWatchService();
        key = dir.register(watcher, ENTRY_CREATE);
        System.out.println("Key " + key.toString());
        commitLogReader = new CommitLogReader();
        commitLogReadHander = new CustomCommitLogReadHandler(configuration);
        System.setProperty("cassandra.config", "file:////etc/cassandra/cassandra.yaml");
        DatabaseDescriptor.forceStaticInitialization();
        Schema.instance.loadFromDisk(false);
    }

    
    /**
     * Process all events for keys queued to the watcher
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public void processEvents() throws InterruptedException, IOException {
    	System.out.println("Entering into processEvents: ");
    	//Path relativePath = ev.context();
        //Path absolutePath = dir.resolve(relativePath);
    	//Path absolutePath = Paths.get("/var/lib/cassandra/commitlog/");
    	Path absolutePath = Paths.get("/var/lib/cassandra/cdc_raw/CommitLog-6-1552988529098.log");
    	processCommitLogSegment(absolutePath);
        //Files.delete(absolutePath);
        /*
    	while (true) {
        	System.out.println("Entering while loop");
            //WatchKey aKey = watcher.take();
//            if (!key.equals(aKey)) {
//                System.out.println("WatchKey not recognized.");
//                continue;
//            }
            for (WatchEvent<?> event : key.pollEvents()) {
            	System.out.println("Entering for loop for watch key");
                WatchEvent.Kind<?> kind = event.kind();
                if (kind != ENTRY_CREATE) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path relativePath = ev.context();
                Path absolutePath = dir.resolve(relativePath);
                processCommitLogSegment(absolutePath);
                Files.delete(absolutePath);

                // print out event
                LOGGER.debug("{}: {}", ev+ent.kind().name(), absolutePath);
            }
            key.reset();
        }*/
    }

    public static void main(String[] args) throws IOException, InterruptedException {
    	System.out.println(args[0]);
        Map<String, Object> configuration = YamlUtils.load(args[0]);
        System.out.println("Reader");
        new Reader(configuration).processEvents();
    }
    
	private void processCommitLogSegment(Path path) throws IOException {
        System.out.println("Processing commitlog segment...");
        commitLogReader.readCommitLogSegment(commitLogReadHander, path.toFile(), false);
        System.out.println("Commitlog segment processed.");
    }
}