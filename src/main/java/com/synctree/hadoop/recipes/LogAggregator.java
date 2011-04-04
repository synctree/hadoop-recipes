package com.synctree.hadoop.recipes;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.filecache.DistributedCache;


public class LogAggregator {
  public static final Pattern LOG_PATTERN = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[(([\\w/]+):([\\d:]+)\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");

  public static class ExtractDateAndIpMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text ip = new Text();

    public void map(Object key, Text value, Context context) 
      throws IOException {

      String text = value.toString();
      Matcher matcher = LOG_PATTERN.matcher(text);
      while (matcher.find()) {
        try {
          ip.set(matcher.group(5) + "\t" + matcher.group(1));
          context.write(ip, one);
        } catch(InterruptedException ex) {
          throw new IOException(ex);
        }
      }

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: LogAggregator <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "LogAggregator");
    job.setJarByClass(LogAggregator.class);
    job.setMapperClass(ExtractDateAndIpMapper.class);
    job.setCombinerClass(WordCount.IntSumReducer.class);
    job.setReducerClass(WordCount.IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
