package com.upgrad.saavntrendproject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SaavnProjectReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		int count = 0;

		for (IntWritable val : value) {
			count = count + val.get();
		}

		try {
			context.write(key, new IntWritable(count));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
