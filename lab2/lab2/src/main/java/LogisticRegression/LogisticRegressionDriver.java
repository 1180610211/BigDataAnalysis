package LogisticRegression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class LogisticRegressionDriver {
    private static Configuration conf = new Configuration();
    private static double[] omega;
    private static double delta = 1E-3;

    public static void main(String[] args) throws Exception {
        conf.setInt("maxIteration", 100);
        conf.setInt("dim", 20);
        conf.setDouble("alpha", 2);
        conf.setDouble("lambda", 2);
        //alpha=0.1,lambda=0.05
        //alpha=0.5,lambda=0.05
        //alpha=1,lambda=0.2
        //alpha=1.5,lambda=0.5
        //alpha=2,lambda=0.5

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        Path output = new Path(args[1]);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = null;
        boolean result;
        int iteration = 0;
        int maxIteration = conf.getInt("maxIteration", 100);

        do {
            System.out.println("iteration" + iteration);
            job = Job.getInstance(conf);
            job.setJarByClass(LogisticRegressionDriver.class);
            // addCacheFiles
            addCacheFiles(job, iteration, args);

            job.setMapperClass(LogisticRegressionMapper.class);
            job.setReducerClass(LogisticRegressionReducer.class);
            job.setPartitionerClass(LogisticRegressionPartitioner.class);

            job.setNumReduceTasks(3);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + "iteration" + iteration));

            result = job.waitForCompletion(true);
            if (!result) {
                throw new Exception("");
            }
            iteration++;
        } while (!isConverged(iteration, args) && iteration < maxIteration);

        System.out.println("success! Logistic Regression complete!");
    }

    public static void addCacheFiles(Job job, int iteration, String[] args) throws IOException, URISyntaxException, InterruptedException {
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        if (iteration == 0) {
            Path path = new Path("hdfs://hadoop100:9000/user/hadoop/lab2/omega.txt");
            int dim = conf.getInt("dim", 20);
            omega = new double[dim + 1];
            if (!fs.exists(path)) {
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
                for (int i = 0; i < omega.length; i++) {
                    bw.write(i + "\t" + omega[i] + "\n");
                }
                bw.close();
            }
            System.out.println("add cache file:" + path);
            job.addCacheFile(path.toUri());
        } else {
            Path path = new Path(args[1] + "/" + "iteration" + (iteration - 1), "part-r-[0-9]*");
            System.out.println("add cache file:" + path);
            FileStatus[] files = fs.globStatus(path);
            for (FileStatus file : files) {
                System.out.println("add cache file:" + file.getPath());
                job.addCacheFile(file.getPath().toUri());
            }
        }
    }

    public static boolean isConverged(int iteration, String[] args) throws Exception {
        //读取这一轮的omega参数值和上一轮的omega参数值进行对比，同时更新omega值
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        Path path = new Path(args[1] + "/" + "iteration" + (iteration - 1), "part-r-[0-9]*");
        FileStatus[] files = fs.globStatus(path);
        double sum = 0;
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split("\t");
                int index = Integer.parseInt(fields[0]);
                double temp = Double.parseDouble(fields[1]);
                sum += Math.abs(omega[index] - temp);
                omega[index] = temp;
            }
        }
        System.out.println("iteration " + iteration + ": " + sum);
        return sum < delta;
    }
}
