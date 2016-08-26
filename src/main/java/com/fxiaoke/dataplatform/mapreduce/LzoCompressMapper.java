package com.fxiaoke.dataplatform.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * LzoCompressMapper
 * Created by wangzk on 16/8/25.
 */
public class LzoCompressMapper extends Mapper<Object, Text, Text, Text> {

  private Log LOGGER = LogFactory.getLog(LzoCompressMapper.class);

  public void map(Object key, Text t_value, Mapper.Context context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit)context.getInputSplit();
    String fileName = fileSplit.getPath().getName();

    context.write(new Text(fileName), t_value);
  }

}
