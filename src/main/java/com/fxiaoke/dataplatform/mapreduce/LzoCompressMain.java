package com.fxiaoke.dataplatform.mapreduce;

import com.fxiaoke.dataplatform.mapreduce.filter.RegexPathFilter;
import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * LzoCompressMain
 * Created by wangzk on 16/8/25.
 */
public class LzoCompressMain extends Configured implements Tool {

  private static Log LOGGER = LogFactory.getLog(LzoCompressMain.class);
  private static final double DEFAULT_FILE_SIZE = 10 * 1024 * 1024 * 1024D; //这是10G, 压缩后大约是2G

  public static void main(String[] args) {
    try {
      int result = ToolRunner.run(new Configuration(), new LzoCompressMain(), args);
      LOGGER.info(result == 0 ? "Job succeed" : "Job fail");
      System.exit(result);
    } catch (Exception e) {
      LOGGER.error("MapReduce failed.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 2) {
      LOGGER.error("Usage: LzoCompressMain <input path> <output path>");
      return -1;
    }

    String inputPath = args[0];
    String outputPath = args[1];

    Configuration conf = this.getConf();
    long pathSize = getDirectorySize(conf, inputPath);
    if (pathSize == 0) {
      LOGGER.error("Path is empty. " + inputPath);
      return -1;
    }

    //设置正则
    conf.set("file.pattern", ".*\\.log");
    // 递归查找
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    Job job = Job.getInstance(conf, "LzoCompressJob");
    // 输入
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
    job.setInputFormatClass(TextInputFormat.class);
    // 中间结果使用lzo压缩
    conf.set("mapreduce.map.output.compress", "true");
    conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
    // 最终结果使用lzop压缩
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "lzo", TextOutputFormat.class, Text.class, Text.class);
    int reduceNums = (int)Math.ceil(pathSize / DEFAULT_FILE_SIZE);
    job.setNumReduceTasks(reduceNums);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setJarByClass(LzoCompressMain.class);
    job.setMapperClass(LzoCompressMapper.class);
    job.setReducerClass(LzoCompressReducer.class);

    boolean result = job.waitForCompletion(true);
    if (!result) {
      LOGGER.info(job.getJobName() + ":" + job.getJobID() + "Failure!");
      return -1;
    }

    // 创建lzo索引
    DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
    lzoIndexer.setConf(conf);
    lzoIndexer.run(new String[]{outputPath});
    return 0;
  }

  // 获取目录大小
  private long getDirectorySize(Configuration conf, String filePath) throws IOException {
    Path dir = new Path(filePath);
    FileSystem fs = dir.getFileSystem(conf);
    return fs.getContentSummary(dir).getLength();
  }

}
