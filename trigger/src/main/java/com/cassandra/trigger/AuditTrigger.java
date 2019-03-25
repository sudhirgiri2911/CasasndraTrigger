package com.cassandra.trigger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
//import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.UUIDGen;
import org.json.simple.JSONObject;


public class AuditTrigger implements ITrigger
{
    //private Properties properties = loadProperties();

	@SuppressWarnings("unchecked")
    public Collection<Mutation> augment(Partition update) 
    {
        String auditKeyspace = "practice";
        String auditTable = "audit_emp";

        //TableMetadata metadata = Schema.instance.getCFMetaData(auditKeyspace, auditTable);
        CFMetaData metadata = Schema.instance.getCFMetaData(auditKeyspace, auditTable);
        PartitionUpdate.SimpleBuilder audit = PartitionUpdate.simpleBuilder(metadata, UUIDGen.getTimeUUID());
        String id = null;
        try {
        	id = new String(update.partitionKey().getKey().array(), "ASCII");
        audit.row()
             .add("keyspace_name", update.metadata().ksName)
             .add("table_name", update.metadata().cfName)
           
             .add("primary_key" , 
            		 new String(update.partitionKey().getKey().array(), "ASCII")
            		 );
    //  .add("primary_key", "emp_id");
            //.add("primary_key", update.metadata().partitionKeyType.getString(update.partitionKey().getKey()));
        }
        catch(UnsupportedEncodingException e) {
        	System.out.println("Exception Occured");
        }
        JSONObject obj = new JSONObject();
		obj.put("id", id);
		obj.put("keyspace_name", update.metadata().ksName);
		obj.put("table_name", update.metadata().cfName);
		FileWriter file = null;
		// try-with-resources statement based on post comment below :)
		try {
			file = new FileWriter("/home/sudhirgiri/triggerFile.txt") ;
			file.write(obj.toJSONString());
			System.out.println("Successfully Copied JSON Object to File...");
			System.out.println("\nJSON Object: " + obj);
			file.close();
		}catch(IOException e) {
		System.out.println("ExceptionOccured");	
		}
        return Collections.singletonList(audit.buildAsMutation());
    }
}
