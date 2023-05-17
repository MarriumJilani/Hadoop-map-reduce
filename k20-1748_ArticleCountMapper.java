package com.articlecount1.wc;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class ArticleCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text yearJournalKey = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] articleFields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); 
        String year = articleFields[2]; // extract the year field
        String journal = articleFields[5]; // extract the journal field
        if (!year.isEmpty() && !journal.isEmpty()) { // check if both fields are not empty
            yearJournalKey.set(year + "," + journal); // set the map output key as "year,journal"
            context.write(yearJournalKey, one); // (yearJournalKey, 1) pair
        }
    }
}

