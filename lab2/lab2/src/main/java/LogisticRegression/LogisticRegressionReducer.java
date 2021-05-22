package LogisticRegression;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class LogisticRegressionReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private int dim;
    private double[] omega;
    private double alpha;
    private double lambda;
    private DoubleWritable doubleWritable = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dim = context.getConfiguration().getInt("dim", 20);
        alpha = context.getConfiguration().getDouble("alpha", 0.1);
        lambda = context.getConfiguration().getDouble("lambda", 0.05);
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
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int cnt = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            cnt++;
        }
        int i = key.get();
        doubleWritable.set(omega[i] - (lambda * omega[i] + alpha * sum) / cnt);
        context.write(key, doubleWritable);
    }
}
