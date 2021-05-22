package KMeans;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<Centroid, Point, Centroid, NullWritable> {
    private double delta;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        delta = context.getConfiguration().getDouble("delta", 0.);
    }

    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        Point temp = null;
        int count = 0;
        for (Point point : values) {
            if (temp == null) {
                temp = new Point(point);
            } else {
                temp.add(point);
            }
            count++;
        }
        temp.average();
        Centroid centroid = new Centroid(key.getLabel(), temp);
        System.out.println("count:" + count);
        System.out.println("distance:" + Point.getDistance(key.getPoint(), centroid.getPoint()));
        System.out.println("centroid:" + centroid);
        if (Point.getDistance(key.getPoint(), centroid.getPoint()) > delta) {
            context.getCounter(KMeansCounter.CHANGED).increment(1);
        }
        context.write(centroid, NullWritable.get());
    }
}
