package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class KMeansTest {
    private static Configuration conf;
    private static FileSystem fs;
    private static double[][] centroids = new double[3][20];

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");

        Path input = new Path("/user/hadoop/lab2/kmeans/iteration11", "part-r-[0-9]*");
        FileStatus[] files = fs.globStatus(input);
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            String[] fields = br.readLine().split("\t");
            int label = Integer.parseInt(fields[0]);
            String[] data = fields[1].split(",");
            for (int i = 0; i < data.length; i++) {
                centroids[label][i] = Double.parseDouble(data[i]);
            }
        }

        calculateLabel();
        System.out.println("K-Means Label Complete!");
    }

    public static int cluster(double[] vector) {
        double distance = Double.MAX_VALUE;
        double currentDistance;
        int label = -1;
        for (int i = 0; i < 3; i++) {
            currentDistance = Point.getDistance(vector, centroids[i]);
            if (currentDistance < distance) {
                distance = currentDistance;
                label = i;
            }
        }
        return label;
    }

    public static void calculateLabel() throws Exception {
        Path input = new Path("/user/hadoop/lab2/聚类数据.txt");
        Path output = new Path("/user/hadoop/lab2/KMeansTestResult.txt");

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
