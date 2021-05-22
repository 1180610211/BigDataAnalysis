package KMeans;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Point implements Writable {
    private ArrayPrimitiveWritable vector = new ArrayPrimitiveWritable();
    private IntWritable num = new IntWritable(0);

    public Point() {
        super();
    }

    public Point(Point point) {
        double[] v = point.getVector();
        this.vector.set(Arrays.copyOf(v, v.length));
        this.num.set(point.getNum());
    }

    public Point(double[] vector, int num) {
        this.vector.set(vector);
        this.num.set(num);
    }

    public Point(String line) {
        String[] data = line.split(",");
        double[] v = new double[data.length];
        for (int i = 0; i < v.length; i++) {
            v[i] = Double.parseDouble(data[i]);
        }
        this.vector.set(v);
    }

    public double[] getVector() {
        return (double[]) vector.get();
    }

    public void setVector(double[] vector) {
        this.vector.set(vector);
    }

    public int getNum() {
        return num.get();
    }

    public void setNum(int num) {
        this.num.set(num);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vector.write(out);
        num.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vector.readFields(in);
        num.readFields(in);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        double[] v = getVector();
        sb.append(v[0]);
        for (int i = 1; i < v.length; i++) {
            sb.append(",");
            sb.append(v[i]);
        }
        return sb.toString();
    }

    public void add(Point point) {
        double[] vector1 = this.getVector();
        double[] vector2 = point.getVector();
        for (int i = 0; i < vector1.length; i++) {
            vector1[i] += vector2[i];
        }
        num.set(this.getNum() + point.getNum());
        vector.set(vector1);
    }

    public void average() {
        double[] temp = this.getVector();
        int number = this.getNum();
        for (int i = 0; i < temp.length; i++) {
            temp[i] = temp[i] / number;
        }
        num.set(1);
        vector.set(temp);
    }

    public static double getDistance(Point point1, Point point2) {
        double distance = 0;
        double[] vector1 = point1.getVector();
        double[] vector2 = point2.getVector();
        for (int i = 0; i < vector1.length; i++) {
            distance += Math.pow((vector1[i] - vector2[i]), 2);
        }
        return distance;
    }

    public static double getDistance(double[] vector1, double[] vector2) {
        double distance = 0;
        for (int i = 0; i < vector1.length; i++) {
            distance += Math.pow((vector1[i] - vector2[i]), 2);
        }
        return distance;
    }
}
