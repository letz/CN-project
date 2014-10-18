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

public class BirdWeightSum {

	public static class BirdWeightMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable weight = new IntWritable(1);
		private Text timeAndStation = new Text();
		private Log log;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				log = new Log(itr.nextToken());
				timeAndStation.set(log.getTowerId() + "," + log.getDate());
				weight.set(Integer.parseInt(log.getWeight()));
				context.write(timeAndStation, weight);
			}
		}
	}

	public static class BirdWeightReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "bird weight sum");
		job.setJarByClass(BirdWeightSum.class);
		job.setMapperClass(BirdWeightMapper.class);
		job.setCombinerClass(BirdWeightReducer.class);
		job.setReducerClass(BirdWeightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}