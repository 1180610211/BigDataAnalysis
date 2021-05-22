package Test.KMeans;

import Test.KMeans.writables.Centroid;
import Test.KMeans.writables.Point;
import org.apache.hadoop.mapreduce.Partitioner;


public class KMeansPartitioner extends Partitioner<Centroid, Point> {
    @Override
    public int getPartition(Centroid key, Point value, int numPartitions) {

        return Integer.valueOf(key.getLabel()) % numPartitions;
    }
}
