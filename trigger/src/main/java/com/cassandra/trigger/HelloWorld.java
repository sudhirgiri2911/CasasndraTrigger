package com.cassandra.trigger;

import java.io.FileWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.json.JSONObject;

public class HelloWorld implements ITrigger
{
    //private static final Logger logger = LoggerFactory.getLogger(HelloWorld.class);
 
	//If we move from cassandra 5.0 to 6.0 and higher we need to change augment to augmentNonBlocking
	public Collection<Mutation> augment(Partition partition)
    {
        String tableName = partition.metadata().cfName;

        JSONObject obj = new JSONObject();
        obj.put("message_id", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));

        try {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            FileWriter file = new FileWriter("/home/sudhirgiri/triggerFile.txt") ;
			file.write(obj.toString());
			System.out.println("Successfully Copied JSON Object to File...");
			System.out.println("\nJSON Object: " + obj);
			file.close();
            while (it.hasNext()) {
                Unfiltered un = it.next();
                Clustering clt = (Clustering) un.clustering();  
                Iterator<Cell> cells = partition.getRow(clt).cells().iterator();
                Iterator<ColumnDefinition> columns = partition.getRow(clt).columns().iterator();

                while(columns.hasNext()){
                    ColumnDefinition columnDef = columns.next();
                    Cell cell = cells.next();
                    String data = new String(cell.value().array()); // If cell type is text
                    obj.put(columnDef.toString(), data);
                    
                    
                }
            }
        } catch (Exception e) {

        }
        return Collections.emptyList();
    }
}