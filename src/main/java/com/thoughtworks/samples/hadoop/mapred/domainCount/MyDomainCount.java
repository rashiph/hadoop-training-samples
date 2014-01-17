package com.thoughtworks.samples.hadoop.mapred.domainCount;

import com.thoughtworks.samples.hadoop.mapred.wordcount.LexicalPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDomainCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MyDomainCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job wordCountJob = new Job(conf, "MyWordCount");
        wordCountJob.setJarByClass(MyDomainCount.class);
        wordCountJob.setMapperClass(DomainMapper.class);
//        if (conf.getBoolean("domainCount.runcombiner", false)) {
            wordCountJob.setCombinerClass(DomainReducer.class);
//        }
//        if (conf.getBoolean("domainCount.partitioner.lexical", true)) {
            wordCountJob.setPartitionerClass(LexicalPartitioner.class);
//        }
        wordCountJob.setReducerClass(DomainReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        wordCountJob.setNumReduceTasks(2);
        wordCountJob.waitForCompletion(true);
        return 0;
    }
}
