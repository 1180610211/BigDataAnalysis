package LogisticRegression;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogisticRegressionPartitioner extends Partitioner<IntWritable, DoubleWritable> {
    @Override
    public int getPartition(IntWritable intWritable, DoubleWritable doubleWritable, int numPartitions) {
        return intWritable.get() % numPartitions;
    }
}
