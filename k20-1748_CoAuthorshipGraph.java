package com.coauthorship.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CoAuthorshipGraph {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (pathArgs.length < 2)
		{
			System.err.println("MR Project Usage: CoAuthorship <input-path> […] <output-path>");
			System.exit(2);
		}
		Job wcJob = Job.getInstance(conf, "MapReduce CoAuthorshipGraph");
		wcJob.setJarByClass(CoAuthorshipGraph.class);
		wcJob.setMapperClass(CoAuthorshipMapper.class);
		wcJob.setCombinerClass(CoAuthorshipReducer.class);
		wcJob.setReducerClass(CoAuthorshipReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		
		for (int i = 0; i < pathArgs.length - 1; ++i)
		{
			FileInputFormat.addInputPath(wcJob, new Path(pathArgs[i]));
		}
		
		FileOutputFormat.setOutputPath(wcJob, new Path(pathArgs[pathArgs.length -1]));
		System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
		}
	}

