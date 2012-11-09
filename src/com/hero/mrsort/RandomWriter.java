package com.hero.mrsort;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomWriter extends Configured implements Tool {
	public static final String TOTAL_BYTES = "mapreduce.randomwriter.totalbytes";
	public static final String BYTES_PER_MAP = "mapreduce.randomwriter.bytespermap";
	public static final String MAPS_PER_HOST = "mapreduce.randomwriter.mapsperhost";
	public static final String MAX_VALUE = "mapreduce.randomwriter.maxvalue";
	public static final String MIN_VALUE = "mapreduce.randomwriter.minvalue";
	public static final String MIN_KEY = "mapreduce.randomwriter.minkey";
	public static final String MAX_KEY = "mapreduce.randomwriter.maxkey";

	/**
	 * User counters
	 */
	static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }

	/**
	 * A custom input format that creates virtual inputs of a single string
	 * for each map.
	 */
	static class RandomInputFormat extends InputFormat<Text, Text> {

		/** 
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			List<InputSplit> result = new ArrayList<InputSplit>();
			Path outDir = FileOutputFormat.getOutputPath(job);
			int numSplits = 
					job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
			for(int i=0; i < numSplits; ++i) {
				result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
						(String[])null));
			}
			return result;
		}

		/**
		 * Return a single record (filename, "") where the filename is taken from
		 * the file split.
		 */
		static class RandomRecordReader extends RecordReader<Text, Text> {
			Path name;
			Text key = null;
			Text value = new Text();
			public RandomRecordReader(Path p) {
				name = p;
			}

			public void initialize(InputSplit split,
					TaskAttemptContext context)
							throws IOException, InterruptedException {

			}

			public boolean nextKeyValue() {
				if (name != null) {
					key = new Text();
					key.set(name.getName());
					name = null;
					return true;
				}
				return false;
			}

			public Text getCurrentKey() {
				return key;
			}

			public Text getCurrentValue() {
				return value;
			}

			public void close() {}

			public float getProgress() {
				return 0.0f;
			}
		}

		public RecordReader<Text, Text> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException, InterruptedException {
			return new RandomRecordReader(((FileSplit) split).getPath());
		}
	}

	static class RandomMapper extends Mapper<WritableComparable, Writable,
	BytesWritable, BytesWritable> {

		private long numBytesToWrite = 128;
		private Random random = new Random();
		private BytesWritable randomKey = new BytesWritable();
		private BytesWritable randomValue = new BytesWritable();

		private void randomizeBytes(byte[] data, int offset, int length) {
			for(int i=offset + length - 1; i >= offset; --i) {
				data[i] = (byte) random.nextInt(256);
			}
		}

		private void setConstantBytes(byte[] data, int offset, int length) {
			for(int i=offset + length - 1; i >= offset; --i) {
				data[i] = 0;
			}
		}


		/**
		 * Given an output filename, write a bunch of random records to it.
		 */
		public void map(WritableComparable key,	Writable value,
				Context context) throws IOException,InterruptedException {
			int itemCount = 0;
			while (numBytesToWrite > 0) {
				int keyLength = 4;
				randomKey.setSize(keyLength);
				randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
				// setConstantBytes(randomKey.getBytes(), 0, randomKey.getLength());
				int valueLength = 4;
				randomValue.setSize(valueLength);
//				randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
				setConstantBytes(randomValue.getBytes(), 0, randomValue.getLength());
				context.write(randomKey, randomValue);
				numBytesToWrite -= keyLength + valueLength;
				context.getCounter(Counters.BYTES_WRITTEN).increment(keyLength + valueLength);
				context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
				context.setStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left.");
			}
			context.setStatus("done with " + itemCount + " records.");
		}

	}

	/**
	 * This is the main routine for launching a distributed random write job.
	 * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
	 * The reduce doesn't do anything.
	 * 
	 * @throws IOException 
	 */
	public int run(String[] args) throws Exception {    

		Path outDir = new Path("/tmp/input");
		Configuration conf = getConf();
		JobClient client = new JobClient(conf);
		ClusterStatus cluster = client.getClusterStatus();
		int numMapsPerHost = conf.getInt(MAPS_PER_HOST, 10);
		long numBytesToWritePerMap = 128;
		long totalBytesToWrite = conf.getLong(TOTAL_BYTES, numMapsPerHost*numBytesToWritePerMap*cluster.getTaskTrackers());
		int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
		if (numMaps == 0 && totalBytesToWrite > 0) {
			numMaps = 1;
			conf.setLong(BYTES_PER_MAP, totalBytesToWrite);
		}
		conf.setInt(MRJobConfig.NUM_MAPS, numMaps);

		Job job = new Job(conf);

		job.setJarByClass(RandomWriter.class);
		job.setJobName("random-writer");
		FileOutputFormat.setOutputPath(job, outDir);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(RandomInputFormat.class);
		job.setMapperClass(RandomMapper.class);        
		job.setReducerClass(Reducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		System.out.println("Running " + numMaps + " maps.");

		// reducer NONE
		job.setNumReduceTasks(0);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		int ret = job.waitForCompletion(true) ? 0 : 1;
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The job took " + 
				(endTime.getTime() - startTime.getTime()) /1000 + 
				" seconds.");

		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RandomWriter(), args);
		System.exit(res);
	}

}
