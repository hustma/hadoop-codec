package com.fxiaoke.dataplatform.benchmark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * TokenizerMapper
 * Created by wangzk on 16/8/26.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

  LongWritable one = new LongWritable(1);
  Text word = new Text();

  public void map(Object key, Text value, Mapper.Context context)
      throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      context.write(word, one);
    }
  }
}