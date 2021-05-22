package KMeans;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner  extends Reducer<Centroid, Point, Centroid, Point> {
    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
