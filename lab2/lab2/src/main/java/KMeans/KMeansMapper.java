package KMeans;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {
    private Point point = new Point();
    private List<Centroid> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            List<URI> uris = Arrays.asList(context.getCacheFiles());
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"), context.getConfiguration(), "hadoop");
            for (URI uri : uris) {
                FSDataInputStream in = fs.open(new Path(uri));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String line;
                while ((line = br.readLine()) != null) {
                    centroids.add(new Centroid(line));
                    System.out.println(line);
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
        point.setVector(vector);
        point.setNum(1);

        Centroid centroid = null;
        double minDistance = Double.MAX_VALUE;
        double currentDistance;

        for (int i = 0; i < centroids.size(); i++) {
            currentDistance = Point.getDistance(centroids.get(i).getPoint(), point);
            if (currentDistance < minDistance) {
                centroid = centroids.get(i);
                minDistance = currentDistance;
            }
        }
        context.write(centroid, point);
    }
}
