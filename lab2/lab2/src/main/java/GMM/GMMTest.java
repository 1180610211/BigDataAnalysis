package GMM;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class GMMTest {
    private static Configuration conf;
    private static FileSystem fs;

    private static double[] pi = new double[3];
    private static double[][] mu = new double[3][20];
    private static double[][][] sigma = new double[3][20][20];
    private static MultivariateNormalDistribution[] distributions = new MultivariateNormalDistribution[3];

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");

        Path input = new Path("/user/hadoop/lab2/gmm/iteration2", "part-r-[0-9]*");
        FileStatus[] files = fs.globStatus(input);
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            int label = Integer.parseInt(br.readLine());
            pi[label] = Double.parseDouble(br.readLine());
            String[] data = br.readLine().split(" ");
            for (int i = 0; i < data.length; i++) {
                mu[label][i] = Double.parseDouble(data[i]);
            }
            for (int i = 0; i < 20; i++) {
                data = br.readLine().split(" ");
                for (int j = 0; j < 20; j++) {
                    sigma[label][i][j] = Double.parseDouble(data[j]);
                }
            }
        }
        for (int i = 0; i < 3; i++) {
            distributions[i] = new MultivariateNormalDistribution(mu[i], sigma[i]);
        }

        calculateLabel();
        System.out.println("GMM Label Complete!");
    }

    public static int cluster(double[] vector) {
        int label = -1;
        double gammaMax = 0;
        double currentGamma;
        for (int i = 0; i < 3; i++) {
            currentGamma = distributions[i].density(vector) * pi[i];
            if (currentGamma > gammaMax) {
                gammaMax = currentGamma;
                label = i;
            }
        }
        return label;
    }

    public static void calculateLabel() throws Exception {
        Path input = new Path("/user/hadoop/lab2/聚类数据.txt");
        Path output = new Path("/user/hadoop/lab2/GMMTestResult.txt");

        FSDataInputStream fis = fs.open(input);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        FSDataOutputStream fos = fs.create(output);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        String line;
        double[] vector = new double[20];
        int predict;

        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");
            for (int i = 0; i < 20; i++) {
                vector[i] = Double.parseDouble(data[i]);
            }
            predict = cluster(vector);
            bw.write(predict + "\n");
        }
    }
}
