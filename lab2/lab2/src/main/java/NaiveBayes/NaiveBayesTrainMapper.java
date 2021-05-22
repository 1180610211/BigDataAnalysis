package NaiveBayes;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NaiveBayesTrainMapper extends Mapper<LongWritable, Text, IntWritable, ArrayPrimitiveWritable> {
    private IntWritable intWritable = new IntWritable();
    private ArrayPrimitiveWritable arrayPrimitiveWritable = new ArrayPrimitiveWritable(double.class);
    private int dim;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dim = context.getConfiguration().getInt("dim", 20);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        double[] vector = new double[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = Double.parseDouble(data[i]);
        }
        intWritable.set(Integer.parseInt(data[dim]));
        arrayPrimitiveWritable.set(vector);
        context.write(intWritable, arrayPrimitiveWritable);
    }
}
