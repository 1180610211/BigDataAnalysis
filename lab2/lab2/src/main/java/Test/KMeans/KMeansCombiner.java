package Test.KMeans;

import Test.KMeans.distances.Distance;
import Test.KMeans.writables.Centroid;

import Test.KMeans.writables.Point;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {
    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        Point result = Distance.sumPoints(values);

        if (result != null){
            context.write(key, result);
        }
    }
}
