package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


/**
 * An example demonstrating k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaKMeansExample
 * </pre>
 */
public class Test {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        String path = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\similarity.txt";
        JavaRDD<String> data = javaSparkContext.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                (Function<String, Vector>) s -> {
                    String[] sarray = s.split(" ");
                    double[] values = new double[sarray.length];
                    for (int i = 0; i < sarray.length; i++) {
                        values[i] = Double.parseDouble(sarray[i]);
                    }
                    return Vectors.dense(values);
                }
        );
        parsedData.cache();

// Cluster the data into two classes using KMeans
        int numClusters = 5;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Save and load model

        /*
        clusters.save(javaSparkContext.sc(),
                "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\src\\main\\resources\\KMeansModel");
        KMeansModel sameModel = KMeansModel.load(javaSparkContext.sc(),
                "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\src\\main\\resources\\KMeansModel");
                */


        javaSparkContext.stop();
    }
}

