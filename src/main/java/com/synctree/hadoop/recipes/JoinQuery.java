package com.synctree.hadoop.recipes;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.filecache.DistributedCache;


public class JoinQuery {

  public static final int PEOPLE_ID_COLUMN                = 0;
  public static final int PEOPLE_FIRST_NAME_COLUMN        = 1;
  public static final int PEOPLE_LAST_NAME_COLUMN         = 2;
  public static final int PEOPLE_FAVORITE_MOVIE_ID_COLUMN = 3;

  public static final int MOVIES_ID_COLUMN                = 0;
  public static final int MOVIES_NAME_COLUMN              = 1;
  public static final int MOVIES_IMAGE_COLUMN             = 2;

  public static final String DELIMITER = "\t";
  public final static Text BLANK = new Text("");

  public static class SelectAndFilterMapper
    extends Mapper<Object, Text, Text, TextArrayWritable> {

    private Text joinKey = new Text();
    private TextArrayWritable columns = new TextArrayWritable();

    public void map(Object key, Text value, Context context) 
      throws IOException {

      String [] row = value.toString().split(DELIMITER);
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      try {
        if(fileName.startsWith("people")) {
          columns.set( new String [] {
            "people",
            row[PEOPLE_FIRST_NAME_COLUMN], 
            row[PEOPLE_LAST_NAME_COLUMN]
          });
          joinKey.set(row[PEOPLE_FAVORITE_MOVIE_ID_COLUMN]);
        } 
        else if(fileName.startsWith("movies")) {
          columns.set( new String [] {
            "movies",
            row[MOVIES_NAME_COLUMN], 
            row[MOVIES_IMAGE_COLUMN]
          });

          joinKey.set(row[MOVIES_ID_COLUMN]);
        }

        context.write(joinKey, columns);

      } catch(InterruptedException ex) {
        throw new IOException(ex);
      }

    }
  }

  public static class CombineMapsReducer 
       extends Reducer<Text,TextArrayWritable,Text, TextArrayWritable> {
    private TextArrayWritable columns = new TextArrayWritable();

    public void reduce(Text key, Iterable<TextArrayWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      LinkedList<String []> people = new LinkedList<String[]>();
      LinkedList<String []> movies = new LinkedList<String[]>();

      for (TextArrayWritable val : values) {
        String dataset = val.getTextAt(0).toString();
        if(dataset.equals("people")) {
          people.add(new String[] {
            val.getTextAt(1).toString(),
            val.getTextAt(2).toString(),
          });
        }
        if(dataset.equals("movies")) {
          movies.add(new String[] {
            val.getTextAt(1).toString(),
            val.getTextAt(2).toString(),
          });
        }
      }

      for(String[] person : people) {
        for(String[] movie : movies) {
          columns.set(new String[] {
            person[0], person[1],
            movie[0], movie[1]
          });
          context.write(BLANK, columns);
        }
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
    if (otherArgs.length != 3) {
      System.err.println("Usage: " + JoinQuery.class.getName() + " <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, JoinQuery.class.getName());
    job.setJarByClass(JoinQuery.class);
    job.setMapperClass(SelectAndFilterMapper.class);
    job.setReducerClass(CombineMapsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}

