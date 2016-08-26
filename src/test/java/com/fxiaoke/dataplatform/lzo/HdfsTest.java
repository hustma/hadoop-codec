package com.fxiaoke.dataplatform.lzo;

import com.github.autoconf.ConfigFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * HdfsTest
 * Created by wangzk on 16/8/26.
 */
@Ignore
@Slf4j
public class HdfsTest {

  private static DistributedFileSystem defaultFS;

  static {
    System.setProperty("process.profile", "firstshare");
    ConfigFactory.getInstance().getConfig("hdfs2-site", config -> {
      Configuration hdfsConf = new Configuration();
      Map<String,String> all = config.getAll();
      for (Map.Entry<String, String> entry : all.entrySet()) {
        hdfsConf.set(entry.getKey(), entry.getValue());
      }
      try {
        defaultFS = (DistributedFileSystem)FileSystem.get(hdfsConf);
      } catch (IOException e) {
        log.error("Cannot create default FileSystem", e);
      }
    });
  }

  @Test
  public void test() throws IOException {
    long numReduceTasks = getNumReduceTasks("/facishare-data/nginx/nginx.reverse/2016/08/24/");
    System.out.println(numReduceTasks);
  }

  private int getNumReduceTasks(String filePath) throws IOException {
    double fileSize = 1024 * 1024 * 1024D;
    long size = getDirectorySize(filePath);
    System.out.println("Dir size: " + size);
    return (int)Math.ceil(size / fileSize);
  }

  private long getSizeOfPath(String filePath) throws IOException {
    FileStatus[] status = defaultFS.listStatus(new Path(filePath));
    long fileSize = 0L;
    for (FileStatus file : status) {
      if (file.isFile()) {
        fileSize += file.getLen();
      }
      file.getBlockSize();
    }
    return fileSize;
  }

  // 获取目录大小
  private long getDirectorySize(String filePath) throws IOException {
    Path dir = new Path(filePath);
//    FileSystem fs = dir.getFileSystem(conf);
    return defaultFS.getContentSummary(dir).getLength();
  }

}
