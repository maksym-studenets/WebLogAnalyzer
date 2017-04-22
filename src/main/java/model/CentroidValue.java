package model;

/**
 * Defines single centroid value. Is a direct subclass of {@link CentroidVector}
 */
public class CentroidValue extends CentroidVector {
    private double centroid;

    public CentroidValue(int clusterNo, double centroid) {
        this.clusterNo = clusterNo;
        this.centroid = centroid;
    }

    @Override
    public int getClusterNo() {
        return this.clusterNo;
    }

    @Override
    public void setClusterNo(int clusterNo) {
        this.clusterNo = clusterNo;
    }

    public double getCentroid() {
        return centroid;
    }

    public void setCentroid(double centroid) {
        this.centroid = centroid;
    }
}
