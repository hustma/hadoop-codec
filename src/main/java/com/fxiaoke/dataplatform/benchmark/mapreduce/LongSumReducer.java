package com.fxiaoke.dataplatform.benchmark.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * LongSumReducer
 * Created by wangzk on 16/8/26.
 */
public class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  LongWritable result = new LongWritable();

  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}
