package GMM;

import KMeans.Centroid;
import KMeans.Point;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GMMMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
    private IntWritable k = new IntWritable();
    private List<GaussianParameters> gaussianParameters = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int num;
        double pi;
        double[] mu;
        double[][] sigma;
        try {
            List<URI> uris = Arrays.asList(context.getCacheFiles());
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), context.getConfiguration(), "hadoop");
            for (URI uri : uris) {
                FSDataInputStream in = fs.open(new Path(uri));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String line;
                while ((line = br.readLine()) != null && !line.equals("")) {
                    num = Integer.parseInt(line);
                    line = br.readLine();
                    pi = Double.parseDouble(line);
                    line = br.readLine();
                    String[] data = line.split(" ");
                    int dim = data.length;
                    mu = new double[dim];
                    for (int i = 0; i < dim; i++) {
                        mu[i] = Double.parseDouble(data[i]);
                    }
                    sigma = new double[dim][dim];
                    for (int i = 0; i < dim; i++) {
                        line = br.readLine();
                        data = line.split(" ");
                        for (int j = 0; j < dim; j++) {
                            sigma[i][j] = Double.parseDouble(data[j]);
                        }
                    }
                    gaussianParameters.add(new GaussianParameters(num, pi, mu, sigma, true));
                }
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        double[] vector = new double[data.length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = Double.parseDouble(data[i]);
        }
        Tuple[] tuples = new Tuple[gaussianParameters.size()];
        double sum = 0;
        for (int i = 0; i < gaussianParameters.size(); i++) {
            GaussianParameters parameters = gaussianParameters.get(i);
            double gamma = parameters.getDistribution().density(vector) * parameters.getPi();
            tuples[parameters.getNum()] = new Tuple(gamma, vector);
            sum += gamma;
        }
        for (int i = 0; i < gaussianParameters.size(); i++) {
            tuples[i].setGamma(tuples[i].getGamma() / sum);
            k.set(i);
            context.write(k, tuples[i]);
//            System.out.println("k=" + k + ",tuple=" + tuples[i]);
        }
    }
}
