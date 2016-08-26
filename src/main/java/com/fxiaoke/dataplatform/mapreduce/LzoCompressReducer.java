package com.fxiaoke.dataplatform.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * LzoCompressReducer
 * Created by wangzk on 16/8/25.
 */
public class LzoCompressReducer extends Reducer<Text, Text, Text, Text> {

  private Log LOGGER = LogFactory.getLog(LzoCompressReducer.class);
  private MultipleOutputs mos;

  @Override
  public void setup(Context context) {
    mos = new MultipleOutputs<>(context);
  }

  protected void reduce(Text key, Iterable<Text> values,
                        Context context) throws IOException, InterruptedException {
    try {
      for (Text text : values) {
        mos.write("lzo", key, text, getFilePath(key.toString()));
      }
    } catch (Exception e) {
      LOGGER.error("Error in reduce. ", e);
    }
  }

  private String getFilePath(String key) {
    return StringUtils.substringBeforeLast(key, ".");
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }

}
