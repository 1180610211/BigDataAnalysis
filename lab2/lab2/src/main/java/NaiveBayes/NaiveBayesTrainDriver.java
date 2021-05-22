package NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class NaiveBayesTrainDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        Path output = new Path(args[1]);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        conf.setInt("dim", 20);

        Job job = Job.getInstance(conf);
        job.setJarByClass(NaiveBayesTrainDriver.class);

        job.setMapperClass(NaiveBayesTrainMapper.class);
        job.setReducerClass(NaiveBayesTrainReducer.class);
        job.setPartitionerClass(NaiveBayesTrainPartitioner.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        if (result)
            System.out.println("success! Naive Bayes complete!");
        System.exit(result ? 0 : 1);
    }
}
