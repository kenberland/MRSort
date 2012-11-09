package com.hero.mrsort;

import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort<K,V> extends Configured implements Tool {
  private RunningJob jobResult = null;

  public int run(String[] args) throws Exception {

    JobConf jobConf = new JobConf(getConf(), Sort.class);
    jobConf.setJobName("sorter");

    jobConf.setMapperClass(IdentityMapper.class);        
    jobConf.setReducerClass(IdentityReducer.class);

    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    @SuppressWarnings("rawtypes")
	Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
    @SuppressWarnings("rawtypes")
	Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
    @SuppressWarnings("rawtypes")
	Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    Class<? extends Writable> outputValueClass = NullWritable.class;

    jobConf.setNumReduceTasks(1);

    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);

    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);
    
    FileInputFormat.setInputPaths(jobConf, "/tmp/input/" );
    FileOutputFormat.setOutputPath( jobConf, new Path( "/tmp/output" ) );
    
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
	int res = ToolRunner.run(new Configuration(), new Sort(), args);
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
