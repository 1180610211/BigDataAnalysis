package GMM;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class GMMDriver {
    public static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");

        Path output = new Path("/user/hadoop/lab2/gmm");
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Initialization.initial();
        System.out.println("Initialization finished");

        conf.setDouble("delta", 1E-6);
        conf.setInt("maxIteration", 100);

        Job job = null;
        boolean result;
        int iteration = 0;
        int maxIteration = conf.getInt("maxIteration", 100);

        do {
            System.out.println("iteration" + iteration);
            job = Job.getInstance(conf);
            job.setJarByClass(GMMDriver.class);
            // addCacheFiles
            addCacheFiles(job, iteration, args);

            job.setMapperClass(GMMMapper.class);
            job.setReducerClass(GMMReducer.class);
//            job.setCombinerClass(KMeansCombiner.class);
            job.setPartitionerClass(GMMPartitioner.class);

            job.setNumReduceTasks(3);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Tuple.class);
            job.setOutputKeyClass(GaussianParameters.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + "iteration" + iteration));

            result = job.waitForCompletion(true);
            if (!result) {
                throw new Exception("");
            }
            iteration++;
        } while (!isConverged(job) && iteration < maxIteration);
        System.out.println("success! GMM complete!");

    }

    public static void addCacheFiles(Job job, int iteration, String[] args) throws IOException, URISyntaxException, InterruptedException {
        if (iteration == 0) {
            Path path = new Path("hdfs://hadoop100:9000/user/hadoop/lab2/initialParameters.txt");
            System.out.println("add cache file:" + path);
            job.addCacheFile(path.toUri());
        } else {
            Path path = new Path(args[1] + "/" + "iteration" + (iteration - 1), "part-r-[0-9]*");
            System.out.println("add cache file:" + path);
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
            FileStatus[] files = fs.globStatus(path);
            for (FileStatus file : files) {
                System.out.println("add cache file:" + file.getPath());
                job.addCacheFile(file.getPath().toUri());
            }
        }
    }

    public static boolean isConverged(Job job) throws IOException {
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(GMMCounter.CHANGED);
        return counter.getValue() == 0L;
    }
}
