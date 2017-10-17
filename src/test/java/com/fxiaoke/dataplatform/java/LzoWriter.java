package com.fxiaoke.dataplatform.java;

import com.google.common.base.Charsets;

import com.hadoop.compression.lzo.LzoCodec;

import org.apache.hadoop.conf.Configuration;

import java.io.FileOutputStream;
import java.io.OutputStream;

import lombok.extern.slf4j.Slf4j;

/**
 * LzoWriter
 * Created by wangzk on 16/11/18.
 */
@Slf4j
public class LzoWriter {


  private void writeToLzo(String filePath, Configuration conf) {

    String content = "ccccc";

    LzoCodec lzo = new LzoCodec();
    lzo.setConf(conf);
    try (OutputStream out = lzo.createOutputStream(new FileOutputStream(""))) {
      out.write(content.getBytes(Charsets.UTF_8));
    } catch (Exception e) {
      log.error("Cannot write to lzo stream. {}", filePath);
    }

  }

}
