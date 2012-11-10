package com.hero.mrsort;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.FileSplit;
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

public class ConfirmSort<K,V> extends Configured implements Tool {

	private RunningJob jobResult = null;
	private static final Text error = new Text("error");

	static class ValidateMap extends MapReduceBase implements Mapper<BytesWritable, NullWritable, Text, BytesWritable> {

		private BytesWritable lastKey = null;
		private String filename;
		private OutputCollector<Text,BytesWritable> output;

		private String getFilename(FileSplit split) {
			return split.getPath().getName();
		}

		public void map(BytesWritable key, NullWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException {
			if (lastKey == null ){
				lastKey = new BytesWritable();
				this.output = output;
				reporter.setStatus("starting with key: " + key.toString());
				filename = getFilename((FileSplit) reporter.getInputSplit());
				output.collect(new Text(filename + ":begin"), key);
			} else {
				if (key.compareTo(lastKey) < 0) {
					output.collect(error, key);
				}
			}
			lastKey.set(key);
			reporter.setStatus("done.");
		}

		public void close() throws IOException {
			if (lastKey != null) {
				output.collect(new Text( filename + ":end"), lastKey);
			}
		}
	}

	static class ValidateReducer extends MapReduceBase implements Reducer<Text, BytesWritable, Text, BytesWritable> {
		private boolean firstKey = true;
		private Text lastKey = new Text();
		private BytesWritable lastValue = new BytesWritable();

		public void reduce(Text key, Iterator<BytesWritable> values, OutputCollector<Text, BytesWritable> output, Reporter reporter) throws IOException {
			if (error.equals(key)) {
				while(values.hasNext()) {
					output.collect(key, values.next());
				}
			} else {
				BytesWritable value = values.next();
				if (firstKey) {
					firstKey = false;
				} else {
					if (value.compareTo(lastValue) < 0) {
						output.collect(error, value);
					}
				}
				lastKey.set(key);
				lastValue.set(value);
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

  /**
   * Get the last job that was run using this instance.
   * @return the results of the last job that was run
   */
  public RunningJob getResult() {
    return jobResult;
  }
}