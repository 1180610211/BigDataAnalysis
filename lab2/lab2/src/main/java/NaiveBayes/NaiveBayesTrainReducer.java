package NaiveBayes;

import org.apache.commons.math3.analysis.function.Power;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NaiveBayesTrainReducer extends Reducer<IntWritable, ArrayPrimitiveWritable, IntWritable, Text> {
    private Text text = new Text();
    private int dim;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dim = context.getConfiguration().getInt("dim", 20);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
        double[] vector;
        RealVector xVector = MatrixUtils.createRealVector(new double[dim]);
        RealVector x2Vector = MatrixUtils.createRealVector(new double[dim]);
        int cnt = 0;
        for (ArrayPrimitiveWritable value : values) {
            vector = (double[]) value.get();
            RealVector tmp = MatrixUtils.createRealVector(vector);
            xVector = xVector.add(tmp);
            x2Vector = x2Vector.add(tmp.map(new Power(2.0)));
            cnt++;
        }

        RealVector muVector = xVector.mapDivide(cnt);
        RealVector sigmaVector = x2Vector.add(muVector.ebeMultiply(xVector).mapMultiply(-2))
                .mapDivide(cnt)
                .add(muVector.map(new Power(2.0)));


        double[] mu = muVector.toArray();
        double[] sigma = sigmaVector.toArray();
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(cnt + "\n");
        for (int i = 0; i < mu.length; i++) {
            sb.append(mu[i]);
            sb.append(" ");
        }
        sb.append("\n");
        for (int i = 0; i < mu.length; i++) {
            sb.append(sigma[i]);
            sb.append(" ");
        }
        sb.append("\n");

        text.set(sb.toString());
        context.write(key, text);
    }
}
