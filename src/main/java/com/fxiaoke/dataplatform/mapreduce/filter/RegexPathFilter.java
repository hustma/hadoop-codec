package com.fxiaoke.dataplatform.mapreduce.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexPathFilter
 * Created by wangzk on 16/8/26.
 */
public class RegexPathFilter extends Configured implements PathFilter {

  private static Log LOGGER = LogFactory.getLog(RegexPathFilter.class);
  private Pattern pattern;
  private FileSystem fs;

  @Override
  public boolean accept(Path path) {
    try {
      if (fs.isDirectory(path)) {
        return true;
      }
      Matcher m = pattern.matcher(path.toString());
      return m.matches();
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return false;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      try {
        fs = FileSystem.get(conf);
        pattern = Pattern.compile(conf.get("file.pattern"));
      } catch (IOException e) {
        LOGGER.error("Cannot set configuration for filter. ", e);
      }
    }
  }

}
