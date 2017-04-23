package main;

import model.AccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import statistics.Clusters;
import statistics.LogInsights;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * Main class for the Web Log Analyzer project
 */
@Deprecated
public class AppOld {
    private static final String LOG_PATH = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer" +
            "\\src\\main\\resources\\rkc-2.log";
    private static JavaSparkContext javaSparkContext;

    public static void main(String[] args) {
        //launch(args);

        javaSparkContext = initializeSpark();
        LogInsights insights = new LogInsights();
        insights.setLogLines(javaSparkContext.textFile(LOG_PATH));

        System.out.println("Total loglines: ");
        ArrayList<AccessLog> cleanData = new ArrayList<>(insights.getCleanedData());

        cleanData.removeIf(log -> log.getResponseCode() != 200 && log.getContentSize() == 0);
        System.out.println("Size of cleaned data set: " + cleanData.size());

        Clusters clusters = new Clusters(cleanData);
        //clusters.calculateSimilarityMatrix();

        System.out.println("Number of clusters: ");
        Scanner in = new Scanner(System.in);
        int n = in.nextInt();

        System.out.println("K MEANS CLUSTERS: ");
        clusters.clusterKMeans(n, 5);
        //JavaRDD<Vector> parsedData = clusters.getParsedData();
        //HashSet<Double> distinctValues = clusters.getDistinctValues();
        //System.out.println("DISTINCT SIMILARITY MATRIX ELEMENTS: ");
        //distinctValues.forEach(System.out::println);

        //JavaRDD<Integer> predictedRdd = clusters.getkMeansModel().predict(parsedData);
        /*
        List<Vector> parsedDataList = parsedData.collect();
        for (Vector vector : parsedDataList) {
            System.out.println(vector.toArray()[0]);
        }
        */

        /*
        ArrayList<Double> parsedArray = new ArrayList<>(parsedDataList.size());
        for (int i = 0; i < parsedArray.size(); i++) {
            parsedArray.add(parsedDataList.get(i).toArray()[0]);
        }
        */

        //HashSet<Integer> predictedDistinct = new HashSet<>(predictedRdd.collect());
        //predictedDistinct.forEach(System.out::println);

        /*
        List<Integer> predictedPoints =  predictedRdd.collect();
        System.out.println("PREDICTED POINTS: ");
        for (Integer element : predictedPoints) {
            System.out.println(element);
        }
        */
        //clusters.getkMeansModel().predict();


        /*
        System.out.println("--- --- ---");
        System.out.println("BISECTING K MEANS CLUSTERS: ");
        clusters.clusterBisectingKMeans(n);

        System.out.println("--- --- ---");
        System.out.println("GENERAL LOG STATISTICS: ");
        List<String> uniqueIps = insights.getDistictIpAddresses();
        System.out.println("Unique IPs: " + uniqueIps);

        List<String> ipAddressData = insights.getFrequentIpStatistics(30);
        //insights.getFrequentIpStatistics(20);

        System.out.println("More than 20 times: " + ipAddressData);

        TreeMap<String, IpAddressGeoData> ipGeoData = insights.getIpGeoData();
        for (Map.Entry<String, IpAddressGeoData> ipAddress : ipGeoData.entrySet()) {
            System.out.println("IP: " + ipAddress.getValue().getIp() +
                    "; Geographical data: " + ipAddress.getValue().getCity() + ", " +
                    ipAddress.getValue().getCountryName());
        }

        System.out.println("Traffic statistics: " + insights.getTrafficStats());
        List<Tuple2<Integer, Long>> statusCodes = insights.getStatusCodeStatistics();
        List<ResponseCodeData> responseCodeData = new ArrayList<>();

        for (Tuple2<Integer, Long> element : statusCodes) {
            responseCodeData.add(new ResponseCodeData(element._1(), element._2()));
        }

        for (ResponseCodeData entry : responseCodeData) {
            System.out.println("Response code: " + entry.getStatusCode() + "\t count: " + entry.getCount());
        }
        */


        System.out.println("Enter exit to exit: ");
        //Scanner in = new Scanner(System.in);
        String command = in.next();

        if (command.equals("exit")) {
            javaSparkContext.stop();
            in.close();
        }
    }

    public static JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    /**
     * Initializes Apache Spark for performing computations, sets basic properties of
     * the Spark application
     *
     * @return {@link JavaSparkContext} object that is used to perform computation of log statistics
     * and for Data Mining tasks
     */
    private static JavaSparkContext initializeSpark() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        return new JavaSparkContext(sparkConf);
    }

    /*
    @Override
    public void start(Stage primaryStage) throws Exception {
        Parent root = FXMLLoader.load(getClass()
                .getClassLoader()
                .getResource("fxml/MainForm.fxml"));
        Scene scene = new Scene(root);
        primaryStage.setScene(scene);
        primaryStage.setTitle("Web Log Analyzer");
        primaryStage.show();
    }
    */
}
