package GMM;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Tuple implements Writable {
    private double gamma;
    private double[] x;

    public Tuple() {
        super();
    }

    public Tuple(double gamma, double[] x) {
        this.gamma = gamma;
        this.x = x;
    }

    public double getGamma() {
        return gamma;
    }

    public void setGamma(double gamma) {
        this.gamma = gamma;
    }

    public double[] getX() {
        return x;
    }

    public void setX(double[] x) {
        this.x = x;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(gamma);
        out.writeInt(x.length);
        for (int i = 0; i < x.length; i++) {
            out.writeDouble(x[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        gamma = in.readDouble();
        x = new double[in.readInt()];
        for (int i = 0; i < x.length; i++) {
            x[i] = in.readDouble();
        }
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "gamma=" + gamma +
                ", x=" + Arrays.toString(x) +
                '}';
    }
}
