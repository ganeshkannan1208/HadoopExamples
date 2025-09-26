import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KMeansSingle {

    // Mapper
    public static class KMeansMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            // Example: split line by comma
            String[] nums = value.toString().split(",");
            // Example: assign to a cluster (dummy 0 for now)
            output.collect(new IntWritable(0), value);
        }
    }

    // Reducer
    public static class KMeansReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            // Example: just output points per cluster
            while(values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    // Driver
    public static void main(String[] args) throws IOException {
        if(args.length < 2){
            System.err.println("Usage: KMeansSingle <input> <output>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(KMeansSingle.class);
        conf.setJobName("KMeansSingle");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KMeansMapper.class);
        conf.setReducerClass(KMeansReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
