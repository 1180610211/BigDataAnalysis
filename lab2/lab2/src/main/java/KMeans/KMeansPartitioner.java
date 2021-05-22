package KMeans;

import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Centroid, Point> {
    @Override
    public int getPartition(Centroid centroid, Point point, int numPartitions) {
        return Integer.valueOf(centroid.getLabel()) % numPartitions;
    }
}
