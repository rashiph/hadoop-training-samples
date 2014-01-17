package com.thoughtworks.samples.hadoop.mapred.domainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DomainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int wordCount = 0;
        for (IntWritable count : counts) {
            wordCount += count.get();
        }
        context.write(word, new IntWritable(wordCount));
    }
}


