package com.cdc.github;


import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CustomCommitLogReadHandler implements CommitLogReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomCommitLogReadHandler.class);

    private final String keyspace;
    private final String table;

    public CustomCommitLogReadHandler(Map<String, Object> configuration) {
        keyspace = "sunbird";
        table = "user_example";
    }

    @Override
    public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor) {
        System.out.println("Handle mutation started...");
        for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates()) {
            process(partitionUpdate);
        }
        System.out.println("Handle mutation finished...");
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException {
        System.out.println("Handle unrecoverable error called.");
        throw new RuntimeException(exception);
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException {
    	System.out.println("Should skip segment on error.");
        exception.printStackTrace();
        return true;
    }


    @SuppressWarnings("unchecked")
    private void process(Partition partition) {
        System.out.println("Processing partion data ");
       if (!partition.metadata().ksName.equals(keyspace)) {
            //System.out.println("Keyspace should be " + keyspace + " but is "  + partition.metadata().ksName);
            return;
        }
        if (!partition.metadata().cfName.equals(table)) {
        	//System.out.println("Table should be '{} but is '{}'." +  table + "." + partition.metadata().cfName);
            return;
        }
        String key = getKey(partition);
        JSONObject obj = new JSONObject();
        obj.put("key", key);
        if (partitionIsDeleted(partition)) {
            obj.put("partitionDeleted", true);
        } else {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            List<JSONObject> rows = new ArrayList<>();
            while (it.hasNext()) {
                Unfiltered un = it.next();
                if (un.isRow()) {
                    JSONObject jsonRow = new JSONObject();
                    Clustering clustering = (Clustering) un.clustering();
                    String clusteringKey = clustering.toCQLString(partition.metadata());
                    jsonRow.put("clusteringKey", clusteringKey);
                    Row row = partition.getRow(clustering);

                    if (rowIsDeleted(row)) {
                        obj.put("rowDeleted", true);
                    } else {
                        Iterator<Cell> cells = row.cells().iterator();
                        Iterator<ColumnDefinition> columns = row.columns().iterator();
                        List<JSONObject> cellObjects = new ArrayList<>();
                        while (cells.hasNext() && columns.hasNext()) {
                            JSONObject jsonCell = new JSONObject();
                            ColumnDefinition columnDef = columns.next();
                            Cell cell = cells.next();
                            //jsonCell.put("name", columnDef.name.toString());
                            jsonCell.put("name", columnDef.name.toString());
                            if (cell.isTombstone()) {
                                jsonCell.put("deleted", true);
                            } else {
                                String data = columnDef.type.getString(cell.value());
                                jsonCell.put("value", data);
                            }
                            jsonCell.put("time", cell.timestamp());
                            cellObjects.add(jsonCell);
                            
                        }
                        jsonRow.put("cells", cellObjects);
                    }
                    rows.add(jsonRow);
                } else if (un.isRangeTombstoneMarker()) {
                    obj.put("rowRangeDeleted", true);
                    ClusteringBound bound = (ClusteringBound) un.clustering();
                    List<JSONObject> bounds = new ArrayList<>();
                    for (int i = 0; i < bound.size(); i++) {
                        String clusteringBound = partition.metadata().comparator.subtype(i).getString(bound.get(i));
                        JSONObject boundObject = new JSONObject();
                        boundObject.put("clusteringKey", clusteringBound);
                        if (i == bound.size() - 1) {
                            if (bound.kind().isStart()) {
                                boundObject.put("inclusive",
                                        bound.kind() == ClusteringPrefix.Kind.INCL_START_BOUND ? true : false);
                            }
                            if (bound.kind().isEnd()) {
                                boundObject.put("inclusive",
                                        bound.kind() == ClusteringPrefix.Kind.INCL_END_BOUND ? true : false);
                            }
                        }
                        bounds.add(boundObject);
                    }
                    obj.put((bound.kind().isStart() ? "start" : "end"), bounds);
                }
            }
            obj.put("rows", rows);
        }
        LOGGER.debug("Creating json value...");
        String value = obj.toJSONString();
        String fileName = "/home/sudhirgiri/cdclogs.txt"; 
        try { 
            BufferedWriter out = new BufferedWriter( 
                          new FileWriter(fileName, true)); 
            out.write(value + "\n"); 
            out.close(); 
        } 
        catch (IOException e) { 
            System.out.println("Exception Occurred" + e); 
        } 
        System.out.println("The value is :"  + value);
    }

    private boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }
}