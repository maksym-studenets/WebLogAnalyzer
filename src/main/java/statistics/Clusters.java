package statistics;

import main.App;
import model.AccessLog;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;

/**
 * Provides realization of clustering tasks for the log data
 */
public class Clusters {
    private JavaRDD<AccessLog> accessLog;
    private ArrayList<AccessLog> logs;
    private ArrayList<SimilarityVector> similarityVectors = new ArrayList<>();

    private double ipValue = 0.75;
    private double contentSizeValue = 0.3;
    private double endpointValue = 0.8;

    public Clusters(ArrayList<AccessLog> logs) {
        this.accessLog = App.getJavaSparkContext().parallelize(logs);
        this.logs = logs;
    }

    public void displayRdd() {
        System.out.println(accessLog.toString());
    }

    public ArrayList<SimilarityVector> calculateSimilarityVector() {
        double similarity;

        for (int i = 0; i < logs.size(); i++) {
            for (int j = 0; j < logs.size(); j++) {
                if (i == j)
                    continue;

                similarity = 0;
                AccessLog log1 = logs.get(i);
                AccessLog log2 = logs.get(j);

                if (log1.getIpAddress().equals(log2.getIpAddress())) {
                    similarity += ipValue;
                }
                if (log1.getContentSize() == log2.getContentSize()) {
                    similarity += contentSizeValue;
                }
                if (log1.getEndpoint().equals(log2.getEndpoint())) {
                    similarity += endpointValue;
                }

                similarityVectors.add(new SimilarityVector(i, j, similarity));
            }
        }

        return similarityVectors;
    }

}
