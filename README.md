
## 功能

1. 使用lzo压缩指定输入目录下的日志到指定的输出路径，并创建索引。
2. 可以指定日志pattern，如以`log`结尾的日志，patter为`*.log`。
3. 对于输入目录，可以递归查找日志，通常可以按天或按月对冷数据进行压缩。

## 使用方法
使用maven打包，提交任务。

```
hadoop jar hadoop-codec-1.0.0-SNAPSHOT.jar com.fxiaoke.dataplatform.mapreduce.LzoCompressMain -Dfile.pattern=<FilePath> <InputPath> <OutputPath>"
```

# Lzo介绍

## 简述

> LZO is a compression codec which gives better compression and decompression speed than gzip, and also the capability to split. LZO allows this because its composed of many smaller (~256K) blocks of compressed data, allowing jobs to be split along block boundaries, as opposed to gzip where the dictionary for the whole file is written at the top.

> When you specify mapred.output.compression.codec as LzoCodec, hadoop will generate .lzo_deflate files. These contain the raw compressed data without any header, and cannot be decompressed with lzop -d command. Hadoop can read these files in the map phase, but this makes your life hard.

> When you specify LzopCodec as the compression.codec, hadoop will generate .lzo files. These contain the header and can be decompressed using lzop -d

> However, neither .lzo nor .lzo_deflate files are splittable by default. This is where LzoIndexer comes into play. It generates an index file which tells you where the record boundary is. This way, multiple map tasks can process the same file.

> See [this cloudera blog post](http://blog.cloudera.com/blog/2009/11/hadoop-at-twitter-part-1-splittable-lzo-compression/) and [LzoIndexer](https://github.com/twitter/hadoop-lzo/blob/master/src/main/java/com/hadoop/compression/lzo/LzoIndexer.java) for more info.

### 创建索引
lzo格式默认是不支持splittable的，需要为其添加索引文件，才能支持多个map并行对lzo文件进行处理

#### MapReduce输出时创建索引

1. 使用lzo索引生成器

    ```java
    // 使用lzo索引生成器
    LzoIndexer lzoIndexer = new LzoIndexer(conf);
    lzoIndexer.index(new Path(outputPath));
    ```

2. 或者使用分布式索引生成器

    ```java
    DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
    lzoIndexer.setConf(conf);
    lzoIndexer.run(new String[]{outputPath});
    ```

#### 对已经是lzo的文件建立索引

```bash
## 单机版
$ hadoop jar /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer /path/to/lzo/part-00000.lzo

## 分布式版
$ hadoop jar /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.DistributedLzoIndexer /path/to/lzo/part-00000.lzo
```
索引文件与源文件在相同目录下。

### beachmark
使用MapReduce做wordcount

|输入|输入大小|输出大小|cpu|memory|map耗时|reduce耗时|总耗时|
|----|----|----|----|----|----|----|----|
|Text|70G|55G|190|389G|2分4秒|10分15秒|12分24秒|
|Lzo|4.3G|3.1G|36|78G|1分55秒|6分11秒|8分10秒|
|比率|6.14%|5.64%|18.95%|20.05%|92.74%|60.33%|65.86%|

## java

```java
public static void compress(String codecClassName) throws Exception {
    Class<?> codecClass = Class.forName(codecClassName);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
    //指定压缩文件路径
    FSDataOutputStream outputStream = fs.create(new Path(/user/hadoop/text.gz));
    //指定要被压缩的文件路径
    FSDataInputStream in = fs.open(new Path(/user/hadoop/aa.txt));
    //创建压缩输出流
    CompressionOutputStream out = codec.createOutputStream(outputStream);
    IOUtils.copyBytes(in, out, conf);
    IOUtils.closeStream(in);
    IOUtils.closeStream(out);
}
```

## MapRedurce

### 读取lzo文件
```java
job.setInputFormatClass(LzoTextInputFormat.class);
```

### map中间结果使用lzo压缩

```java
conf.set("mapreduce.map.output.compress", "true");
conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
```

### 输出lzo文件
```java
FileOutputFormat.setCompressOutput(job, true);
FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
int result = job.waitForCompletion(true) ? 0 : 1;
// 上面的语句执行完成后，会生成最后的输出文件，需要在此基础上添加lzo的索引
// 使用lzo索引生成器
LzoIndexer lzoIndexer = new LzoIndexer(conf);
lzoIndexer.index(new Path(outputPath));
// 或者使用分布式索引生成器
// DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
// lzoIndexer.setConf(conf);
// lzoIndexer.run(new String[]{outputPath});
```

## Spark
### 读取lzo文件
可以直接读取lzo文件。

```scala
scala> val lzoFile = sc.textFile("/path/to/lzo/*.lzo")
scala> val lzoWordCounts = lzoFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a + b)
scala> lzoWordCounts.collect()
```

### 输出lzo文件
save时指定输出格式，`classOf[com.hadoop.compression.lzo.LzopCodec]`。

```scala
val textFile = sc.textFile("/xxx/in")
textFile.saveAsTextFile("/xxx/out/", classOf[com.hadoop.compression.lzo.LzopCodec])
```

## Hive

1. 创建表时指定为lzo格式

    ```sql
    CREATE EXTERNAL TABLE foo (
         columnA string,
         columnB string
    ) PARTITIONED BY (date string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY "\t"
    STORED AS
    INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
    OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    LOCATION '/path/to/hive/tables/foo';
    ```

2. 对于已经创建好的表，使用alter语句，将其修改为lzo存储格式

    ```sql
    ALTER TABLE foo
    SET FILEFORMAT
    INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
    OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    ```

3. 插入数据时，需要添加下面两个参数

    ```sql
    SET hive.exec.compress.output=true;
    SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
    ```

## 参考
1. [Hadoop at Twitter (part 1): Splittable LZO Compression](http://blog.cloudera.com/blog/2009/11/hadoop-at-twitter-part-1-splittable-lzo-compression/)
2. [Do we need to create an index file (with lzop) if compression type is RECORD instead of block?](http://stackoverflow.com/questions/23560281/do-we-need-to-create-an-index-file-with-lzop-if-compression-type-is-record-ins)
3. [What's the difference between the LzoCodec and the LzopCodec in Hadoop-LZO?](https://www.quora.com/Whats-the-difference-between-the-LzoCodec-and-the-LzopCodec-in-Hadoop-LZO)
4. [HDFS中文件的压缩与解压](http://www.cnblogs.com/liuling/p/2013-6-19-01.html)
5. [Hadoop, how to compress mapper output but not the reducer output](http://stackoverflow.com/questions/5571156/hadoop-how-to-compress-mapper-output-but-not-the-reducer-output)
6. [mapreduce中的压缩](http://blog.csdn.net/lastsweetop/article/details/9187721)
7. [mapred-default.xml](https://hadoop.apache.org/docs/r2.6.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
8. [Hadoop列式存储引擎Parquet/ORC和snappy压缩](http://www.itweet.cn/2016/03/15/columnar-storage-parquet-and-orc/)
9. [IBM Developerworks: Hadoop 压缩实现分析](https://www.ibm.com/developerworks/cn/opensource/os-cn-hadoop-compression-analysis/)