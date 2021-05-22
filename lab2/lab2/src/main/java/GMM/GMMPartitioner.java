package GMM;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GMMPartitioner extends Partitioner<IntWritable, Tuple> {
    @Override
    public int getPartition(IntWritable intWritable, Tuple tuple, int numPartitions) {
        return intWritable.get() % numPartitions;
    }
}
