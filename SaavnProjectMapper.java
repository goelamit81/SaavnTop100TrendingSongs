package com.upgrad.saavntrendproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;

public class SaavnProjectMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static List<String> days = Arrays.asList("2017-12-24", "2017-12-25", "2017-12-26", "2017-12-27",
			"2017-12-28", "2017-12-29", "2017-12-30");
	private static List<String> hours = Arrays.asList("08", "09", "10", "11", "12", "13");
	private static IntWritable ONE = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] st = value.toString().split(",");
		String songId = st[0];
		String streamHour = st[3];
		String streamDate = st[4];
		String str = "";
		if (days.contains(streamDate) && hours.contains(streamHour)) {
			str = streamDate + ":" + songId;
			try {
				context.write(new Text(str), ONE);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
