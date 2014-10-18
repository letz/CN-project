import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class NotSeenBirds {

    public static class NotSeenBirdsMapper extends Mapper<Object, Text, Text, Text>{

        private Text wingspan = new Text();
        private Text mapkey = new Text();

        private Log log;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                log = new Log(itr.nextToken());
                mapkey.set(log.getBirdId());
                wingspan.set(log.getDate());
                context.write(mapkey, wingspan);
            }
        }

    }



    public static class NotSeenBirdsReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/");
            Date date = new Date(0);
            for (Text val : values) {
                Date dateval = new Date(0);
                try {
                    dateval = formatter.parse(val.toString());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                if (date.getTime() < dateval.getTime()) {
                    date = dateval;
                }
            }
            result.set(formatter.format(date));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NotSeenBirds");
        job.setJarByClass(NotSeenBirds.class);
        job.setMapperClass(NotSeenBirdsMapper.class);
        job.setCombinerClass(NotSeenBirdsReducer.class);
        job.setReducerClass(NotSeenBirdsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
