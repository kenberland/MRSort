package com.hero.mrsort;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

public class SequenceReader {

	
	public static void main(String[] args) throws Exception {
				
		SequenceFile.Reader.Option[] options = new SequenceFile.Reader.Option[1];
		options[0] = SequenceFile.Reader.file( new Path("/tmp/output/part-m-00000") );	
		SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), options );
		
		BytesWritable key = new BytesWritable();
		BytesWritable val = new BytesWritable();
		ByteBuffer myBuffer = ByteBuffer.allocate(4);
		
		while( reader.next(key, val) ){
			myBuffer.rewind();
			myBuffer.put( key.getBytes(), 0, 4 );
			myBuffer.rewind();
			System.out.print( myBuffer.getInt() );
			myBuffer.rewind();
			myBuffer.put( val.getBytes(), 0, 4 );
			myBuffer.rewind();
			System.out.print( ":" + myBuffer.getInt() + "\n");

		}		
		reader.close();
		System.err.println("Done");

	}

}
