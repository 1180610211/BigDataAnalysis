package GMM;

import KMeans.Point;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GaussianParameters implements WritableComparable<GaussianParameters> {
    private int num;
    private double pi;
    private double[] mu;
    private double[][] sigma;
    private MultivariateNormalDistribution distribution;

    public GaussianParameters() {
        super();
    }

    public GaussianParameters(int num, double[] mu) {
        this.num = num;
        this.mu = mu;
        this.pi = 0;
        this.sigma = new double[mu.length][mu.length];
    }

    public GaussianParameters(int num, double pi, double[] mu, double[][] sigma, boolean dist) {
        this.num = num;
        this.pi = pi;
        this.mu = mu;
        this.sigma = sigma;
        if (dist == true) {
            distribution = new MultivariateNormalDistribution(mu, sigma);
        }
    }

    public void addNewPoint(Point point) {
        RealMatrix sigmaMatrix = MatrixUtils.createRealMatrix(sigma);
        RealMatrix x = MatrixUtils.createColumnRealMatrix(point.getVector());
        RealMatrix mu = MatrixUtils.createColumnRealMatrix(this.mu);
        RealMatrix t = x.subtract(mu);
        sigma = sigmaMatrix.add(t.multiply(t.transpose())).getData();
        this.pi++;
    }

    public void divide(int num) {
        int m = sigma.length, n = sigma[0].length;
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                sigma[i][j] /= pi;
            }
        }
        pi = pi / num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public double getPi() {
        return pi;
    }

    public void setPi(double pi) {
        this.pi = pi;
    }

    public double[] getMu() {
        return mu;
    }

    public void setMu(double[] mu) {
        this.mu = mu;
    }

    public double[][] getSigma() {
        return sigma;
    }

    public void setSigma(double[][] sigma) {
        this.sigma = sigma;
    }

    public MultivariateNormalDistribution getDistribution() {
        return distribution;
    }

    public void setDistribution(MultivariateNormalDistribution distribution) {
        this.distribution = distribution;
    }

    /*
            private int num;
            private double pi;
            private double[] mu;
            private RealMatrix sigma;
         */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(num);
        out.writeDouble(pi);

        out.writeInt(mu.length);
        for (int i = 0; i < mu.length; i++) {
            out.writeDouble(mu[i]);
        }

        int m = sigma.length, n = sigma[0].length;
        out.writeInt(m);
        out.writeInt(n);
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                out.writeDouble(sigma[m][n]);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        num = in.readInt();
        pi = in.readDouble();

        int l = in.readInt();
        mu = new double[l];
        for (int i = 0; i < l; i++) {
            mu[i] = in.readDouble();
        }

        int m = in.readInt(), n = in.readInt();
        double[][] sigma = new double[m][n];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                sigma[i][j] = in.readDouble();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(num);
        sb.append("\n");
        sb.append(pi);
        sb.append("\n");

        for (int i = 0; i < mu.length; i++) {
            sb.append(mu[i]);
            sb.append(" ");
        }
        sb.append("\n");

        int m = sigma.length, n = sigma[0].length;
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                sb.append(sigma[i][j]);
                sb.append(" ");
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    @Override
    public int compareTo(GaussianParameters o) {
        return Integer.compare(num, o.getNum());
    }
}
