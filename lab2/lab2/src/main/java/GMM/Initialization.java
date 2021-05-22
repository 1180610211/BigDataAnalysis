package GMM;

import KMeans.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 初始化 GMM 的初始参数
 */
public class Initialization {

    public static void initial() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");

        //聚类数目
        int k = 3;
        int dim = 20;

        //K-means算法的最后一次迭代得到的centroids
        Path input1 = new Path("/user/hadoop/lab2/kmeans/iteration11", "part-r-[0-9]*");
        Path input2 = new Path("/user/hadoop/lab2/聚类数据.txt");
        Path output = new Path("/user/hadoop/lab2/initialParameters.txt");
        Path iteration = new Path("/user/hadoop/lab2/gmm");

        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        if (fs.exists(iteration)) {
            fs.delete(iteration, true);
        }

        FSDataInputStream fis = fs.open(input2);
        FSDataOutputStream fos = fs.create(output);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));


        String line = null;
        int label;
        Centroid[] centroids = new Centroid[k];
        Centroid centroid;
        GaussianParameters[] gaussianParameters = new GaussianParameters[k];

        FileStatus[] files = fs.globStatus(input1);
        for (FileStatus file : files) {
            BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            while ((line = br2.readLine()) != null) {
                centroid = new Centroid(line);
                label = Integer.parseInt(centroid.getLabel());
                centroids[label] = centroid;
                gaussianParameters[label] = new GaussianParameters(label, centroid.getPoint().getVector());
            }
        }


        //利用K-means计算得到的结果初始化参数
//        int cnt = 0;
//        double minDistance;
//        double currentDistance;
//        while ((line = br.readLine()) != null) {
//            Point point = new Point(line);
//            centroid = null;
//            minDistance = Double.MAX_VALUE;
//            for (int i = 0; i < centroids.length; i++) {
//                currentDistance = Point.getDistance(centroids[i].getPoint(), point);
//                if (currentDistance < minDistance) {
//                    centroid = centroids[i];
//                    minDistance = currentDistance;
//                }
//            }
//            label = Integer.parseInt(centroid.getLabel());
//            gaussianParameters[label].addNewPoint(point);
//            cnt++;
//        }


        //单位矩阵I
        double[][] sigma = new double[dim][dim];
        for (int i = 0; i < dim; i++) {
            sigma[i][i] = 1;
        }

        for (int i = 0; i < k; i++) {
//            gaussianParameters[i].divide(cnt);
            gaussianParameters[i].setPi(1.0 / k);
            gaussianParameters[i].setSigma(sigma);
            bw.write(gaussianParameters[i].toString());
        }

        bw.close();
        br.close();
        fs.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        initial();
    }

}
