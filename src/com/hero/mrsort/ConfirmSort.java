package com.hero.mrsort;

import java.io.IOException;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;
import java.util.UUID;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ConfirmSort<K,V> extends Configured implements Tool {
	
	private static Logger logger = Logger.getLogger("ConfirmSort.class");
	private RunningJob jobResult = null;
	private static final Text error = new Text("Turdstein");
	static enum Markers { BEGIN, END } ;

	static class ValidateMap extends MapReduceBase implements Mapper<BytesWritable, NullWritable, Text, BytesWritable> {

		private BytesWritable lastKey = null;
		private String filename;
		private OutputCollector<Text,BytesWritable> output;
		static enum Counters { NULL_LAST_KEYS };

		public void map(BytesWritable key, NullWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException {
			if (lastKey == null ){
				lastKey = new BytesWritable();
				reporter.incrCounter(Counters.NULL_LAST_KEYS, 1);
				this.output = output;
				reporter.setStatus("starting with key: " + key.toString());
				filename = UUID.randomUUID().toString();
				output.collect(new Text(filename + ":" + Markers.BEGIN ), key);
			} else {
				if (key.compareTo(lastKey) < 0) {
					output.collect(error, key);
					logger.error("Error in mapper: key=" + key + ", lastKey=" + lastKey);
				}
			}
			lastKey.set(key);
		}

		public void close() throws IOException {
			output.collect(new Text( filename + ":" + Markers.END ), lastKey);
			lastKey = null;
		}
	}

	static class ValidateReducer extends MapReduceBase implements Reducer<Text, BytesWritable, Text, BytesWritable> {
		
		private HashMap<String, HashMap<String, Long>> myRangesData = new HashMap<String, HashMap<String, Long>>();
		private OutputCollector<Text,BytesWritable> output;

		public void reduce(Text key, Iterator<BytesWritable> values, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException {
			if (error.equals(key)) {
				while(values.hasNext()) {
					output.collect(key, values.next());
				}
			} else {
				// bucket the begin and end keys in a structure to review at the end
				BytesWritable value;
				while ( values.hasNext() ){
					value = values.next();										
					String[] keySplit = MrUtils.splitOnColon(key.toString());
					if ( ! myRangesData.containsKey( keySplit[0] ) ){
						myRangesData.put(keySplit[0], new HashMap<String, Long>() );
					}
					myRangesData.get( keySplit[0] ).put(keySplit[1], MrUtils.unsignedIntToLong(value.getBytes()) );
				}
			}
		}
		
		public void close() throws IOException {
			// convert the data to ranges and make sure none of the ranges overlap
			System.out.println(myRangesData.toString());
			Stack<LongRange> myRanges = new Stack<LongRange>();
			Iterator <String> myyRangesIterator = myRangesData.keySet().iterator();
			// convert the data to an ArrayList of Range's
			while( myyRangesIterator.hasNext() ){
				String myKey = myyRangesIterator.next();
				// Don't have to set() with .toString, but ya gotta get() that way.  Go figure.
				myRanges.push( new LongRange( myRangesData.get(myKey).get(Markers.BEGIN.toString()), myRangesData.get(myKey).get(Markers.END.toString() ) ) );
				logger.error("Make range: l=" + myRangesData.get(myKey).get(Markers.BEGIN.toString()) + ", h=" + myRangesData.get(myKey).get(Markers.END.toString()) );
			}
			// Check the first range against all the others, run time is Σ(n-1)i=1i, like factorial but with addition, what's that called?  4+3+2+1...
			try {
				while(true){
					LongRange heroRange = myRanges.pop();
					Iterator<LongRange> otherRangesIterator =  myRanges.iterator();
					while( otherRangesIterator.hasNext() ){
						LongRange otherRange = otherRangesIterator.next();
						if ( heroRange.overlapsRange(otherRange) ){
							logger.error("Sort is fucked. hero=" + heroRange.toString() + " otherRange=" + otherRange.toString() );
							BytesWritable value = null;
							output.collect(error, value);
						}
					}
				}
			} catch ( EmptyStackException e ){
				logger.error("stack is empty.");
			}
		}

	}



	public int run(String[] args) throws Exception {

    JobConf jobConf = new JobConf(getConf(), ConfirmSort.class);
    jobConf.setJobName("confirm-sort");

    jobConf.setMapperClass(ValidateMap.class);        
    jobConf.setReducerClass(ValidateReducer.class);

    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    @SuppressWarnings("rawtypes")
	Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
    @SuppressWarnings("rawtypes")
	Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
    @SuppressWarnings("rawtypes")
	Class<? extends WritableComparable> outputKeyClass = Text.class;
    Class<? extends Writable> outputValueClass = BytesWritable.class;

	jobConf.setNumMapTasks(4);
    jobConf.setNumReduceTasks(1);

    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);

    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);
    
    FileInputFormat.setInputPaths(jobConf, "/tmp/output" );
    FileOutputFormat.setOutputPath( jobConf, new Path( "/tmp/confirm" ) );
    
    System.out.println("Running on " +
    		cluster.getTaskTrackers() +
        " nodes to sort from " + 
        FileInputFormat.getInputPaths(jobConf)[0] + " into " +
        FileOutputFormat.getOutputPath(jobConf) +
        " with " + 1 + " reduces.");
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    jobResult = JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " + 
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return 0;
  }



  public static void main(String[] args) throws Exception {
    @SuppressWarnings("rawtypes")
	int res = ToolRunner.run(new Configuration(), new ConfirmSort(), args);
    System.exit(res);
  }

  public RunningJob getResult() {
    return jobResult;
  }
  
}
