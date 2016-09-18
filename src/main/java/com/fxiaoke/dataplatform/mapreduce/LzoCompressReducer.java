package com.fxiaoke.dataplatform.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * LzoCompressReducer
 * Created by wangzk on 16/8/25.
 */
public class LzoCompressReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Log LOGGER = LogFactory.getLog(LzoCompressReducer.class);
  private MultipleOutputs mos;

  @Override
  public void setup(Context context) {
    LOGGER.info("Reducer start to set up...");
    mos = new MultipleOutputs<>(context);
  }

  protected void reduce(Text key, Iterable<Text> values,
                        Context context) throws IOException, InterruptedException {
    try {
      for (Text text : values) {
        String filePath = getFilePath(key.toString(), context.getConfiguration().get("lzo.name", "lzo"));
        mos.write("lzo", NullWritable.get(), text, filePath);
      }
    } catch (Exception e) {
      LOGGER.error("Error in reduce. ", e);
    }
  }

  private String getFilePath(String key, String defaultKey) {
    if (StringUtils.contains(key, ".")) {
      return StringUtils.substringBeforeLast(key, ".");
    }
    return defaultKey;
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }

}
