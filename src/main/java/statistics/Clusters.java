package statistics;

import main.App;
import model.AccessLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * Provides realization of clustering tasks for the log data
 */
public class Clusters {
    private JavaRDD<AccessLog> accessLog;
    private ArrayList<AccessLog> logs;
    //private SimilarityValue similarityVector = new SimilarityValue();

    private double ipValue = 0.75;
    private double contentSizeValue = 0.3;
    private double endpointValue = 0.8;
    private double dateValue = 0.15;

    public Clusters(ArrayList<AccessLog> logs) {
        this.accessLog = App.getJavaSparkContext().parallelize(logs);
        this.logs = logs;
    }

    public void displayRdd() {
        System.out.println(accessLog.toString());
    }

    public void calculateSimilarityVector() {
        double similarity;

        try {
            File file = new File("similarity.txt");
            FileWriter fileWriter = new FileWriter(file);


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
                    if ((log1.getDate().get(Calendar.HOUR) == log2.getDate().get(Calendar.HOUR) &&
                            (log1.getDate().get(Calendar.MINUTE) == log2.getDate()
                                    .get(Calendar.MINUTE)))) {
                        similarity += dateValue;
                    }

                    SimilarityValue similarityValue = new SimilarityValue(i, j, similarity);
                    fileWriter.write(similarityValue.getRow1() + " " +
                            similarityValue.getRow2() + " " + similarityValue.getValue());
                    fileWriter.write("\n");
                }
            }
            fileWriter.flush();
            fileWriter.close();

            System.out.println("Ended computing similarity");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clusterKMeans(int clusterCount) {
        String path = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\similarity.txt";
        JavaRDD<String> data = App.getJavaSparkContext().textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sArray = s.split(" ");
            double[] values = new double[sArray.length];
            for (int i = 0; i < sArray.length; i++) {
                values[i] = Double.parseDouble(sArray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        int iterationNumber = 10;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), clusterCount, iterationNumber);

        System.out.println("Cluster centers: ");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(center);
        }

        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }
    /*
    @Deprecated
    public void clusterDataKMeans(int clusterCount) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("K-Means Sample")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().format("libsvm").load("D:\\Progs\\JAVA\\" +
                "2017\\2\\webloganalyzer\\src\\main\\resources\\kmeans_data.txt");
        KMeans kMeans = new KMeans().setK(clusterCount).setSeed(1L);
        KMeansModel kMeansModel = kMeans.fit(dataset);

        double WSSSE = kMeansModel.computeCost(dataset);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        Vector[] centers = kMeansModel.clusterCenters();
        System.out.println("Cluster centers: ");
        for (Vector center : centers) {
            System.out.println(center);
        }

        sparkSession.stop();
    }
    */
}
