package com.hero.mrsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

public class SequenceReaderAndConfirmation {

	public static final long unsignedIntToLong(byte[] b) 
	{
	    long l = 0;
	    l |= b[0] & 0xFF;
	    l <<= 8;
	    l |= b[1] & 0xFF;
	    l <<= 8;
	    l |= b[2] & 0xFF;
	    l <<= 8;
	    l |= b[3] & 0xFF;
	    return l;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/tmp/output/part-00000"), new Configuration());

		BytesWritable key = new BytesWritable();
		Long cur;
		Long last=0L;
		while( reader.next(key) ){
			cur = unsignedIntToLong( key.getBytes() );
			if (cur < last ){
				System.err.println("Your sort is busted.");
				System.exit(-2);
			}
			last = cur;
		}		
		reader.close();
		System.err.println("SUCCESS");
		System.exit(0);
	}

}
