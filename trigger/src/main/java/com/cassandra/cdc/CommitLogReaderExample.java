package com.cassandra.cdc;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
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


public class CommitLogReaderExample 
{
	public static void main(String... args) {
//		Config.setClientMode(false);
		Logger logger = LoggerFactory.getLogger(CommitLogReaderExample.class);
        logger.info("This is how you configure Java Logging with SLF4J");
		System.setProperty("cassandra.config", "file:////etc/cassandra/cassandra.yaml");
		CommitLogReader clr = new CommitLogReader();
		File file = new File("/home/sudhirgiri/able/CommitLog-6-1552460439931.log");
//	    File [] fs = {new File("/home/sudhirgiri/able/CommitLog-6-1552460439931.log") , 
//	    		new File("/home/sudhirgiri/able/CommitLog-6-1552470134974.log")};
		try {
			System.out.println("Hey");
			//CommitLogReplayer handler = CommitLogReplayer.construct(CommitLog.instance);
			CommitLogReadHandler handler = new CustomCommitLogReadHandler();
	    clr.readCommitLogSegment( handler,
	    		file, 
	    		false);
	    //handler.replayPath(file, false);
	    
	    System.out.println("completed");
	    //System.exit(0);
		}
		catch(IOException e) {
			System.out.println("Exception Occured");
		}
	}	
}