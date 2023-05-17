package com.coauthorship.wc;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class CoAuthorshipMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    
		String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // split line into columns
	    if (line.length > 1) { // ignore empty lines
	        String[] authors = line[1].replaceAll("\\[|\\]|'|\"", "").split(", "); // extract authors and remove punctuation
	        Arrays.sort(authors); // sort authors alphabetically to avoid duplicate pairs
	        for (int i = 0; i < authors.length - 1; i++) {
	            for (int j = i + 1; j < authors.length; j++) {
	                Text authorPair = new Text(authors[i] + "," + authors[j]);
	                context.write(authorPair, one);
	            }
	        }
	    }
	}

  }

 