package com.hero.mrsort;


import java.io.IOException;
import java.util.Date;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomWriter extends Configured implements Tool {

	static class RandomInputFormat implements InputFormat<Text, Text> {

		/** 
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public InputSplit[] getSplits(JobConf job, 
				int numSplits) throws IOException {
			InputSplit[] result = new InputSplit[numSplits];
			Path outDir = FileOutputFormat.getOutputPath(job);
			for(int i=0; i < result.length; ++i) {
				result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, (String[])null);
			}
			return result;
		}

		/**
		 * Return a single record (filename, "") where the filename is taken from
		 * the file split.
		 */
		static class RandomRecordReader implements RecordReader<Text, Text> {
			Path name;
			public RandomRecordReader(Path p) {
				name = p;
			}
			public boolean next(Text key, Text value) {
				if (name != null) {
					key.set(name.getName());
					name = null;
					return true;
				}
				return false;
			}
			public Text createKey() {
				return new Text();
			}
			public Text createValue() {
				return new Text();
			}
			public long getPos() {
				return 0;
			}
			public void close() {}
			public float getProgress() {
				return 0.0f;
			}
		}

		public RecordReader<Text, Text> getRecordReader(InputSplit split,
				JobConf job, 
				Reporter reporter) throws IOException {
			return new RandomRecordReader(((FileSplit) split).getPath());
		}
	}

	@SuppressWarnings("rawtypes")
	static class Map extends MapReduceBase
	implements Mapper<WritableComparable, Writable,
	BytesWritable, NullWritable> {

		private long numBytesToWrite;
		private Random random = new Random();
		private BytesWritable randomKey = new BytesWritable();

		private void randomizeBytes(byte[] data, int offset, int length) {
			for(int i=offset + length - 1; i >= offset; --i) {
				data[i] = (byte) random.nextInt(256);
			}
		}


		public void map(WritableComparable key, 
				Writable value,
				OutputCollector<BytesWritable, NullWritable> output, 
				Reporter reporter) throws IOException {
			int itemCount = 0;
			while (numBytesToWrite > 0) {
				randomKey.setSize(4);
				randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
				output.collect(randomKey, NullWritable.get());
				numBytesToWrite -= 4;
				if (++itemCount % 200 == 0) {
					reporter.setStatus("wrote record " + itemCount + ". " + 
							numBytesToWrite + " bytes left.");
				}
			}
			reporter.setStatus("done with " + itemCount + " records.");
		}


		@Override
		public void configure(JobConf job) {
			numBytesToWrite = 32 * 1024 * 1024;
		}

	}


	public int run(String[] args) throws Exception {    

		Path outDir = new Path("/tmp/input");
		JobConf job = new JobConf(getConf());

		job.setJarByClass(RandomWriter.class);
		job.setJobName("random-writer");
		FileOutputFormat.setOutputPath(job, outDir);

		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormat(RandomInputFormat.class);
		job.setMapperClass(Map.class);        
		job.setReducerClass(IdentityReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		int numMaps = 4;
		job.setNumMapTasks(numMaps);
		System.out.println("Running " + numMaps + " maps.");    
		// reducer NONE
		job.setNumReduceTasks(0);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(job);
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The job took " + 
				(endTime.getTime() - startTime.getTime()) /1000 + 
				" seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RandomWriter(), args);
		System.exit(res);
	}

}
