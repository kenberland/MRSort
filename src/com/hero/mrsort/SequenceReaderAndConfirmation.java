package com.hero.mrsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

public class SequenceReaderAndConfirmation {
	
	private static Logger logger = Logger.getLogger("SequenceReaderAndConfirmation.class");
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/tmp/output/part-00000"), new Configuration());

		BytesWritable key = new BytesWritable();
		Long cur;
		Long last=0L;
		while( reader.next(key) ){
			cur = MrUtils.unsignedIntToLong(key.getBytes() );
			logger.error(cur);
			if (cur < last ){
				logger.error("Your sort is busted.");
				System.exit(-2);
			}
			last = cur;
		}		
		reader.close();
		logger.error("SUCCESS");
		System.exit(0);
	}

}
