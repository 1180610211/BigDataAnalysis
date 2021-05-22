package LogisticRegression;

import KMeans.Centroid;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class LogisticRegressionMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private IntWritable intWritable = new IntWritable();
    private DoubleWritable doubleWritable = new DoubleWritable();
    private int dim;
    private double[] omega;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dim = context.getConfiguration().getInt("dim", 20);
        omega = new double[dim + 1];
        try {
            List<URI> uris = Arrays.asList(context.getCacheFiles());
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), context.getConfiguration(), "hadoop");
            for (URI uri : uris) {
                FSDataInputStream in = fs.open(new Path(uri));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    omega[Integer.parseInt(fields[0])] = Double.parseDouble(fields[1]);
                }
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        double[] x = new double[dim + 1];
        double y;

        x[0] = 1;//偏置项b
        for (int i = 0; i < dim; i++) {
            x[i + 1] = Double.parseDouble(data[i]);
        }
        y = Double.parseDouble(data[dim]);

        double temp = 0;
        for (int i = 0; i < dim + 1; i++) {
            temp += omega[i] * x[i];
        }
        double sigmoid = 1 - (1 / (1 + Math.exp(temp)));

        for (int i = 0; i < dim + 1; i++) {
            intWritable.set(i);
            doubleWritable.set((sigmoid - y) * x[i]);
            context.write(intWritable, doubleWritable);
        }
    }
}
