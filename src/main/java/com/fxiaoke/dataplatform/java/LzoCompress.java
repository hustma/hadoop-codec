package com.fxiaoke.dataplatform.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * LzoCodec
 * Created by wangzk on 16/8/25.
 */
public class LzoCompress {

  public static void compress(Configuration conf, String inPath, String outPath, Class codecClass) throws Exception {
//    Class<?> codecClass = Class.forName(codecClassName);
    FileSystem fs = FileSystem.get(conf);
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    //指定要被压缩的文件路径
    FSDataInputStream in = fs.open(new Path(inPath));
    //指定压缩文件路径
    FSDataOutputStream outputStream = fs.create(new Path(outPath));
    //创建压缩输出流
    CompressionOutputStream out = codec.createOutputStream(outputStream);
    IOUtils.copyBytes(in, out, conf);
    IOUtils.closeStream(in);
    IOUtils.closeStream(out);
  }

}
