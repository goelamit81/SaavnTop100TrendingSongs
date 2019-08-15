package com.upgrad.saavntrendproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class SaavnProjectDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new Configuration(), new SaavnProjectDriver(), args);
		System.exit(returnStatus);
	}

	public int run(String[] args) throws IOException {
		Path inputFile = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		long totalInputRecordCount;
		long totalOutputRecordCount;
		Job job;
		Configuration conf = new Configuration();

		int returnCode = 0;
		job = Job.getInstance(conf, "Saavn Project");
		job.setJarByClass(SaavnProjectDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(SaavnProjectMapper.class);
		job.setReducerClass(SaavnProjectReducer.class);
		job.setCombinerClass(SaavnProjectReducer.class);
		job.setPartitionerClass(SaavnProjectPartitioner.class);
		job.setNumReduceTasks(7);
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputDir);

		try {
			returnCode = job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (returnCode == 0) {
			totalInputRecordCount = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
			totalOutputRecordCount = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

			System.out.println("Total Input Record : " + totalInputRecordCount);
			System.out.println("Total Output Record : " + totalOutputRecordCount);
			System.out.println("MapReduce Job is Success");

		}

		return returnCode;
	}
}
