package KMeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid> {
    private Text label = new Text();
    private Point point = new Point();

    public Centroid() {
        super();
    }

    public Centroid(String label, Point point) {
        this.label.set(label);
        this.point = point;
    }

    public Centroid(String value) {
        String[] fields = value.split("\\t");
        label.set(fields[0]);
        String[] data = fields[1].split(",");
        double[] vector = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            vector[i] = Double.parseDouble(data[i]);
        }
        point.setVector(vector);
        point.setNum(1);
    }

    public String getLabel() {
        return label.toString();
    }

    public void setLabel(String label) {
        this.label.set(label);
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        label.write(out);
        point.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        label.readFields(in);
        point.readFields(in);
    }

    @Override
    public String toString() {
        return getLabel() + "\t" + point.toString();
    }

    @Override
    public int compareTo(Centroid o) {
        return getLabel().compareTo(o.getLabel());
    }
}
