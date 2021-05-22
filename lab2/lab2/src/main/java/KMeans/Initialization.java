package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * 初始化 K-Means 的centroids质心的坐标
 */
public class Initialization {

    public static void initial() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "hadoop");

        Path input = new Path("/user/hadoop/lab2/聚类数据.txt");
        Path output = new Path("/user/hadoop/lab2/centroids.txt");
        Path iteration = new Path("/user/hadoop/lab2/kmeans");

        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        if (fs.exists(iteration)) {
            fs.delete(iteration, true);
        }

        FSDataInputStream fis = fs.open(input);
        FSDataOutputStream fos = fs.create(output);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        Random random = new Random();
        String line = null;
        int cnt = 0;
        int randomNum = 0;
        String[] centroids = new String[3];

        while ((line = br.readLine()) != null) {
            if (cnt < 3) {
                centroids[cnt] = line;
            } else {
                randomNum = random.nextInt(cnt + 1);
                if (randomNum < 3) {
                    centroids[randomNum] = line;
                }
            }
            cnt++;
        }

        for (int i = 0; i < centroids.length; i++) {
            bw.write(i + "\t" + centroids[i] + "\n");
        }
        bw.close();
        br.close();
        fs.close();
    }
}
