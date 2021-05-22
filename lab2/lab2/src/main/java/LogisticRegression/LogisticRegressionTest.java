package LogisticRegression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class LogisticRegressionTest {
    private static Configuration conf;
    private static FileSystem fs;
    private static double[] omega = new double[21];


    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");
        ///user/hadoop/lab2/LogisticRegression5/iteration36
        ///user/hadoop/lab2/LogisticRegression6/iteration12
        //

        Path input = new Path("/user/hadoop/lab2/LogisticRegression3/iteration83", "part-r-[0-9]*");
        FileStatus[] files = fs.globStatus(input);
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split("\t");
                omega[Integer.parseInt(fields[0])] = Double.parseDouble(fields[1]);
            }
        }
//        calculateScore();
        calculateLabel();
    }

    public static int classify(double[] vector) {
        double sum = omega[0];
        for (int i = 0; i < 20; i++) {
            sum += omega[i + 1] * vector[i];
        }
        if (sum >= 0) return 1;
        else return 0;
    }

    public static void calculateLabel() throws Exception {
        Path input = new Path("/user/hadoop/lab2/测试数据.txt");
        Path output = new Path("/user/hadoop/lab2/LogisticRegressionTestResult.txt");

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
        System.out.println("Logistic Regression classification complete!");
    }

    public static void calculateScore() throws Exception {
//        Path input = new Path("/user/hadoop/lab2/训练数据.txt");
        Path input = new Path("/user/hadoop/lab2/验证数据.txt");

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
