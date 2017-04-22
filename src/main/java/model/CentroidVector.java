package model;

import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Defines a model for a clustering algorithm centroid vectors
 */
public class CentroidVector {
    protected int clusterNo;
    private String points;

    private Vector pointsVector;

    public CentroidVector() {
        this.clusterNo = 0;
        this.pointsVector = null;
    }

    public CentroidVector(int clusterNo, Vector points) {
        this.clusterNo = clusterNo;
        this.pointsVector = points;
        this.points = getPointsArray();
    }

    public int getClusterNo() {
        return clusterNo;
    }

    public void setClusterNo(int clusterNo) {
        this.clusterNo = clusterNo;
    }

    public Vector getPointsVector() {
        return pointsVector;
    }

    public String getPointsArray() {
        double[] temp = pointsVector.toArray();
        List<String> pointsArray = new ArrayList<>(temp.length);
        //String[] pointString = new String[temp.length];
        for (int i = 0; i < temp.length; i++) {
            pointsArray.add(String.valueOf(temp[i]));
        }

        return pointsArray.stream().collect(Collectors.joining("\n"));
    }

    public void setPointsVector(Vector pointsVector) {
        this.pointsVector = pointsVector;
    }

    public String getPoints() {
        return points;
    }

    public void setPoints(String points) {
        this.points = points;
    }
}
