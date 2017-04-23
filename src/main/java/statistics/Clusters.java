package statistics;

import main.App;
import model.AccessLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;

/**
 * Provides realization of clustering tasks for the log data
 */
public class Clusters implements Serializable {
    private ArrayList<AccessLog> logs;
    private JavaRDD<Vector> parsedData;
    private HashSet<Double> distinctValues = new HashSet<>();

    private KMeansModel kMeansModel;
    private ArrayList<Vector> kMeansCentroids;
    private double kMeansCost;

    private ArrayList<Vector> bisectingKMeansCentroids;
    private double bisectingKMeansCost;


    public Clusters(ArrayList<AccessLog> logs) {
        this.logs = logs;
        retrieveData();
    }

    public void calculateSimilarityMatrix() {
        double similarity;

        try {
            File file = new File("similarity.txt");
            FileWriter fileWriter = new FileWriter(file);


            for (int i = 0; i < logs.size(); i++) {
                for (int j = 1; j < logs.size(); j++) {

                    similarity = 0;
                    AccessLog log1 = logs.get(i);
                    AccessLog log2 = logs.get(j);

                    if (log1.getIpAddress().equals(log2.getIpAddress())) {
                        double ipValue = 0.75;
                        similarity += ipValue;
                    }
                    if (log1.getContentSize() == log2.getContentSize()) {
                        double contentSizeValue = 0.3;
                        similarity += contentSizeValue;
                    }
                    if (log1.getEndpoint().equals(log2.getEndpoint())) {
                        double endpointValue = 0.8;
                        similarity += endpointValue;
                    }
                    if ((log1.getDate().get(Calendar.HOUR) == log2.getDate().get(Calendar.HOUR) &&
                            (log1.getDate().get(Calendar.MINUTE) == log2.getDate()
                                    .get(Calendar.MINUTE)))) {
                        double dateValue = 0.15;
                        similarity += dateValue;
                    }

                    fileWriter.write(String.valueOf(similarity));
                    fileWriter.write(" ");
                }
                fileWriter.write("\n");
            }
            fileWriter.flush();
            fileWriter.close();

            System.out.println("Ended computing similarity");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clusterKMeans(int clusterCount, int iterationCount) {
        RDD<Vector> parsedRdd = parsedData.rdd();
        kMeansModel = KMeans.train(parsedRdd, clusterCount, iterationCount);
        kMeansCentroids = new ArrayList<>(Arrays.asList(kMeansModel.clusterCenters()));
        kMeansCost = kMeansModel.computeCost(parsedRdd);
    }

    public void clusterBisectingKMeans(int clusterCount) {
        BisectingKMeans bisectingKMeans = new BisectingKMeans()
                .setK(clusterCount);
        BisectingKMeansModel bisectingKMeansModel = bisectingKMeans.run(parsedData);
        bisectingKMeansCentroids = new ArrayList<>(Arrays.asList(bisectingKMeansModel
                .clusterCenters()));
        bisectingKMeansCost = bisectingKMeansModel.computeCost(parsedData);
    }

    private void retrieveData() {
        String path = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\similarity.txt";
        JavaRDD<String> data = App.getJavaSparkContext().textFile(path);
        parsedData = data.map(s -> {
            String[] sArray = s.split(" ");
            double[] values = new double[sArray.length];
            for (int i = 0; i < sArray.length; i++) {
                values[i] = Double.parseDouble(sArray[i]);
            }
            //List<Double> doubles = Doubles.asList(values);
            return Vectors.dense(values);
        });
        parsedData.cache();
    }

    /**
     * @since 1.2
     */
    public JavaRDD<Vector> getParsedData() {
        return parsedData;
    }

    /**
     * @since 1.2
     */
    public KMeansModel getkMeansModel() {
        return kMeansModel;
    }

    public ArrayList<Vector> getkMeansCentroids() {
        return kMeansCentroids;
    }

    public double getkMeansCost() {
        return kMeansCost;
    }

    public ArrayList<Vector> getBisectingKMeansCentroids() {
        return bisectingKMeansCentroids;
    }

    public double getBisectingKMeansCost() {
        return bisectingKMeansCost;
    }
}
