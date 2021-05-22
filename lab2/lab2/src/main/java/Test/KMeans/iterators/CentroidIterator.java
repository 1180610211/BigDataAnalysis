package Test.KMeans.iterators;


import Test.KMeans.distances.Distance;
import Test.KMeans.writables.Centroid;
import Test.KMeans.writables.Point;

import java.util.ArrayList;
import java.util.List;

public class CentroidIterator {
    private List<Centroid> centroids;
    private Distance distance;

    public CentroidIterator(Distance distance){
        centroids = new ArrayList<>();
        this.distance = distance;
    }
    public void add(String record){
        centroids.add(new Centroid(record));
    }

    public Centroid closest(Point point) throws Exception {
        if (centroids.size() <= 0) return null;

        Centroid closest = centroids.get(0);
        double minDist = distance.getDistance(closest.getPoint(), point);
        for (Centroid centroid: centroids){
            double dist = distance.getDistance(centroid.getPoint(), point);
            if (minDist >= dist){
                closest = centroid;
                minDist = dist;
            }
        }

        return closest;
    }
}
