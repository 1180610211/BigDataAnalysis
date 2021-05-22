package NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class NaiverBayesTest {
    private static Configuration conf;
    private static FileSystem fs;
    private static int[] pi = new int[2];
    private static double[][] mu = new double[2][20];
    private static double[][] sigma2 = new double[2][20];

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        Path input = new Path("/user/hadoop/lab2/NaiveBayes", "part-r-[0-9]*");
        FileStatus[] files = fs.globStatus(input);
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            int label = Integer.parseInt(br.readLine().trim());
            pi[label] = Integer.parseInt(br.readLine());
            String[] data1 = br.readLine().split(" ");
            String[] data2 = br.readLine().split(" ");
            for (int i = 0; i < data1.length; i++) {
                mu[label][i] = Double.parseDouble(data1[i]);
                sigma2[label][i] = Double.parseDouble(data2[i]);
            }
        }
        calculateScore();
//        calculateLabel();
    }

    public static int classify(double[] vector) {
        double PP0 = pi[0], PP1 = pi[1];
        for (int i = 0; i < 20; i++) {
            PP0 *= 1 / (Math.sqrt(2 * Math.PI * sigma2[0][i])) * Math.exp(-Math.pow(vector[i] - mu[0][i], 2) / (2 * sigma2[0][i]));
            PP1 *= 1 / (Math.sqrt(2 * Math.PI * sigma2[1][i])) * Math.exp(-Math.pow(vector[i] - mu[1][i], 2) / (2 * sigma2[1][i]));
        }
        if (PP0 < PP1) return 1;
        else return 0;
    }

    public static void calculateLabel() throws Exception {
        Path input = new Path("/user/hadoop/lab2/测试数据.txt");
        Path output = new Path("/user/hadoop/lab2/NaiveBayesTestResult.txt");

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
            predict = classify(vector);
            bw.write(predict + "\n");
        }
        System.out.println("Naive Bayes classification complete!");
    }

    public static void calculateScore() throws Exception {
        Path input = new Path("/user/hadoop/lab2/训练数据.txt");
//        Path input = new Path("/user/hadoop/lab2/验证数据.txt");

        FSDataInputStream fis = fs.open(input);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line;
        double[] vector = new double[20];
        int label;
        int predict;
        int cnt = 0;
        double truePositive, falsePositive, trueNegative, falseNegative;
        truePositive = falsePositive = trueNegative = falseNegative = 0;
        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");
            for (int i = 0; i < 20; i++) {
                vector[i] = Double.parseDouble(data[i]);
            }
            label = Integer.parseInt(data[20]);
            predict = classify(vector);

            if (label == 1 && predict == 1) truePositive++;
            else if (label == 1 && predict == 0) falseNegative++;
            else if (label == 0 && predict == 0) trueNegative++;
            else falsePositive++;

            cnt++;
        }

        System.out.println("truePositive:" + truePositive);
        System.out.println("falsePositive:" + falsePositive);
        System.out.println("trueNegative:" + trueNegative);
        System.out.println("falseNegative:" + falseNegative);

        double P = (truePositive / (truePositive + falsePositive));
        double R = (truePositive / (truePositive + falseNegative));

        System.out.println("\nAccuracy:" + ((truePositive + trueNegative) / cnt));
        System.out.println("Precision:" + P);
        System.out.println("Recall:" + R);
        System.out.println("\nF1-Score:" + (2 * P * R / (P + R)));
    }
}
