package com.synctree.hadoop.recipes;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.filecache.DistributedCache;


public class SimpleQuery {

  public static final int ID_COLUMN                = 0;
  public static final int FIRST_NAME_COLUMN        = 1;
  public static final int LAST_NAME_COLUMN         = 2;
  public static final int FAVORITE_MOVIE_ID_COLUMN = 3;
  public static final String DELIMITER = "\t";

  public static class SelectAndFilterMapper
    extends Mapper<Object, Text, TextArrayWritable, Text> {

    private final static Text blank = new Text("");
    private TextArrayWritable columns = new TextArrayWritable();

    public void map(Object key, Text value, Context context) 
      throws IOException {

      String [] row = value.toString().split(DELIMITER);

      try {
        if( row[FIRST_NAME_COLUMN].equals("John") ||
            row[FAVORITE_MOVIE_ID_COLUMN].equals("2") ) {

          columns.set( new String[] { row[FIRST_NAME_COLUMN], row[LAST_NAME_COLUMN] });

          context.write(columns, blank);

        }
      } catch(InterruptedException ex) {
        throw new IOException(ex);
      }

    }
  }

  public static class TextArrayWritable extends ArrayWritable 
    implements WritableComparable<TextArrayWritable> {

    public TextArrayWritable() {
      super(Text.class);
    }

    public void set(String[]columns) {
      Text[] t = new Text[columns.length];
      for(int i = 0; i < columns.length; i++) {
        t[i] = new Text(columns[i]);
      }
      set(t);
    }

    public Text getTextAt(int i) {
      Text t = (Text)(((Writable[])get())[i]);
      return t;
    }

    public int compareTo(TextArrayWritable w) {
      for(int i = 0; i < get().length; i++) {
        
        int c = getTextAt(i).compareTo(w.getTextAt(i));
        if(c != 0) return c;
      }
      
      return 0;
    }

    public String toString () {
      final StringBuilder sb = new StringBuilder();
      final Writable[] strings = get();

      for (int i = 0; i < strings.length; i++) {
        if (i > 0) { sb.append('\t'); }
        sb.append((Text) strings[i]);
      }
      return sb.toString();
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: " + SimpleQuery.class.getName() + " <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, SimpleQuery.class.getName());
    job.setJarByClass(SimpleQuery.class);
    job.setMapperClass(SelectAndFilterMapper.class);
    job.setOutputKeyClass(TextArrayWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
