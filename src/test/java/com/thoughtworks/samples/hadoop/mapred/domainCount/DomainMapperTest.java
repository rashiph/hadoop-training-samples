package com.thoughtworks.samples.hadoop.mapred.domainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class DomainMapperTest {
  @Test
  public void should_count_each_domain_as_one_occurrence() throws IOException, InterruptedException {
    DomainMapper domainMapper= new DomainMapper();
    LongWritable dummyKey = new LongWritable();
    Text inputValue = new Text("a@hotmail.com");
    Mapper.Context context = mock(Mapper.Context.class);
    domainMapper.map(dummyKey, inputValue, context);

    verify(context, times(1)).write(new Text("hotmail.com"), new IntWritable(1));
  }

}
