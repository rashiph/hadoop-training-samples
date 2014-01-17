package com.thoughtworks.samples.hadoop.mapred.domainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DomainMapper extends Mapper<Object, Text, Text, IntWritable> {

  @Override
  public void map(Object key, Text text, Context context)
      throws IOException, InterruptedException {
    String line = text.toString();
    String token = line.split("@")[1];
    context.write(new Text(token), new IntWritable(1));
  }
}