package com.upgrad.saavntrendproject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;

public class SaavnProjectPartitioner extends Partitioner<Text, IntWritable> implements Configurable {

	private Configuration configuration;
	private HashMap<String, Integer> days = new HashMap<String, Integer>();

	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		days.put("2017-12-24", 0);
		days.put("2017-12-25", 1);
		days.put("2017-12-26", 2);
		days.put("2017-12-27", 3);
		days.put("2017-12-28", 4);
		days.put("2017-12-29", 5);
		days.put("2017-12-30", 6);
	}

	public Configuration getConf() {
		return configuration;
	}

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String streamDate = key.toString().split(":")[0];

		return (int) (days.get(streamDate));
	}
}
