package NaiveBayes;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaiveBayesTrainPartitioner extends Partitioner<IntWritable, ArrayPrimitiveWritable> {
    @Override
    public int getPartition(IntWritable intWritable, ArrayPrimitiveWritable arrayPrimitiveWritable, int numPartitions) {
        return intWritable.get() % numPartitions;
    }
}
