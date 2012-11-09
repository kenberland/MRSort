package com.hero.mrsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;

public class SequenceWriter {

	public static void main(String[] args) throws IOException {
		ByteWritable key = new ByteWritable();
		ByteWritable val = new ByteWritable();		
//		SequenceFile.Writer.Option[] options = new SequenceFile.Writer.Option[4];
//		options[0] = SequenceFile.Writer.file( new Path("/tmp/input") );
//		options[1] = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);	
//		options[2] = SequenceFile.Writer.keyClass( key.getClass() );
//		options[3] = SequenceFile.Writer.valueClass( val.getClass() );
//		SequenceFile.Writer myWriter = SequenceFile.createWriter(new Configuration(), options );
//		for ( int i=0; i < 10; i++ ){
//			int myRandomInteger = (int) Math.floor(Math.random() * 255 );
//			byte myByte = (byte) myRandomInteger;
//			key.set(myByte);
//			val.set(myByte);
//			myWriter.append(key, val);
//		}
//		myWriter.close();
		System.err.println("Done");
	}
}
