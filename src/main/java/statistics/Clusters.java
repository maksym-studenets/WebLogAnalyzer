package statistics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vector;

/**
 * Provides realization of clustering tasks for the log data
 */
public class Clusters {

    private JavaRDD<Vector> data1;
    private JavaRDD<Vector> data2;
    private JavaRDD<String> logLines;

    public Clusters(JavaRDD<String> logLines) {
        this.logLines = logLines;
    }

    /*
    public void doKMeansClustering(int clustersCount, int iterationsCount) {
        prepareVectorKMeans();
        KMeansModel kMeansModel = KMeans.train(data1, clustersCount, iterationsCount);

        System.out.println("Cluster centers: ");
        for (Vector center : kMeansModel.clusterCenters()) {
            System.out.println(center);
        }
    }

    public void doBisectingKMeansClustering() {
        prepareVectorBisectingKMeans();

    }

    private void prepareVectorKMeans() {
        data1 = logLines.
                map((Function<String, Vector>) s -> null);
    }

    private void prepareVectorBisectingKMeans() {
        data2 = logLines.
                map((Function<String, Vector>) s -> null);
    }
    */
}
