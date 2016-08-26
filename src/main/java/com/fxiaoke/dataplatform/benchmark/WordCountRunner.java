package com.fxiaoke.dataplatform.benchmark;

import com.fxiaoke.dataplatform.mapreduce.filter.RegexPathFilter;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCountRunner
 * Created by wangzk on 16/8/26.
 */
public class WordCountRunner extends Configured implements Tool {
  private static Log LOGGER = LogFactory.getLog(WordCountRunner.class);

  public static void main(String[] args) {
    try {
      int result = ToolRunner.run(new Configuration(), new WordCountRunner(), args);
      System.exit(result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 3) {
      LOGGER.error("Usage: WordCountRunner <inputPath> <outputPath> <suffix>");
      return -1;
    }

    String inputPath = args[0];
    String outputPath = args[1];
    String suffix = args[2];

    Configuration conf = this.getConf();

    //设置正则
    conf.set("file.pattern", ".*\\." + suffix);
    // 递归查找
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    Job job = Job.getInstance(conf, "WordCountBenchMark");
    // 输入
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
    if ("lzo".equals(suffix)) {
      job.setInputFormatClass(LzoTextInputFormat.class);
    } else {
      job.setInputFormatClass(TextInputFormat.class);
    }
    // 中间结果使用lzo压缩
    conf.set("mapreduce.map.output.compress", "true");
    conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
    // 最终结果
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setJarByClass(WordCountRunner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    boolean result = job.waitForCompletion(true);
    if (!result) {
      LOGGER.info(job.getJobName() + ":" + job.getJobID() + "Failure!");
      return -1;
    }
    return 0;
  }

}
