package GMM;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GMMReducer extends Reducer<IntWritable, Tuple, GaussianParameters, NullWritable> {
    private int dim;
    private double delta;
    private List<GaussianParameters> gaussianParameters = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dim = context.getConfiguration().getInt("dim", 20);
        delta = context.getConfiguration().getDouble("delta", 0.);

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
                    gaussianParameters.add(new GaussianParameters(num, pi, mu, sigma, false));
                }
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        double gammaSum = 0;
        RealMatrix gammaX = MatrixUtils.createColumnRealMatrix(new double[dim]);
        RealMatrix gammaXXT = MatrixUtils.createRealMatrix(dim, dim);
        for (Tuple tuple : values) {
            RealMatrix x = MatrixUtils.createColumnRealMatrix(tuple.getX());
            double gamma = tuple.getGamma();
            gammaSum += gamma;
            gammaX = gammaX.add(x.scalarMultiply(gamma));
            gammaXXT = gammaXXT.add(x.multiply(x.transpose()).scalarMultiply(gamma));
            count++;
        }

        double[] mu = gammaX.getColumn(0);
        for (int i = 0; i < mu.length; i++) {
            mu[i] /= gammaSum;
        }
        RealMatrix muMatrix = MatrixUtils.createColumnRealMatrix(mu);

        double[][] sigma = gammaXXT
                .add(muMatrix.multiply(gammaX.transpose()).scalarMultiply(-1))
                .add(gammaX.multiply(muMatrix.transpose()).scalarMultiply(-1))
                .add(muMatrix.multiply(muMatrix.transpose()).scalarMultiply(gammaSum))
                .getData();
        for (int i = 0; i < sigma.length; i++) {
            for (int j = 0; j < sigma[0].length; j++) {
                sigma[i][j] /= gammaSum;
            }
        }

        if (Math.abs(new LUDecomposition(MatrixUtils.createRealMatrix(sigma)).getDeterminant()) < delta) {
            for (int i = 0; i < dim; i++) {
                sigma[i][i] += delta;
            }
        }

        double pi = gammaSum / count;
        for (int i = 0; i < gaussianParameters.size(); i++) {
            GaussianParameters parameters = gaussianParameters.get(i);
            if (parameters.getNum() == key.get()) {
                if (Math.abs(pi - parameters.getPi()) > delta) {
                    context.getCounter(GMMCounter.CHANGED).increment(1);
                }
                double[] mu2 = parameters.getMu();
                for (int j = 0; j < mu.length; j++) {
                    if (Math.abs(mu[j] - mu2[j]) > delta) {
                        context.getCounter(GMMCounter.CHANGED).increment(1);
                    }
                }

                double[][] sigma2 = parameters.getSigma();
                for (int j = 0; j < sigma2.length; j++) {
                    for (int k = 0; k < sigma2[0].length; k++) {
                        if (Math.abs(sigma[j][k] - sigma2[j][k]) > delta) {
                            context.getCounter(GMMCounter.CHANGED).increment(1);
                        }
                    }
                }
            }
        }

        context.write(new GaussianParameters(key.get(), pi, mu, sigma, false), NullWritable.get());
    }
}
