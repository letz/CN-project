import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BiggestWingSpan {

  public static class WingSpanMapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable wingspan = new IntWritable();
    private Text mapkey = new Text();
    private Log log;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        log = new Log(itr.nextToken());
        mapkey.set(log.getTowerId() + "," + log.getDate() + "," + log.getWeather());
        wingspan.set(Integer.valueOf(log.getWingSpan()));
        context.write(mapkey, wingspan);
      }
    }

  }

  public static class WingSpanReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int max = 0;
      for (IntWritable val : values) {
        if(val.get() > max) {
          max = val.get();
        }
      }
      result.set(max);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "BiggestWingSpan");
    job.setJarByClass(BiggestWingSpan.class);
    job.setMapperClass(WingSpanMapper.class);
    job.setCombinerClass(WingSpanReducer.class);
    job.setReducerClass(WingSpanReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
