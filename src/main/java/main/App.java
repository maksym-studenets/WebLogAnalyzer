package main;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import model.AccessLog;
import model.IpAddressGeoData;
import model.StatusData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import statistics.Clusters;
import statistics.LogInsights;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * Main class for the Web Log Analyzer project
 */

/* TODO: ENABLE JAVAFX INTEGRATION WITH GRADLE. COMMENTED BELOW
 */

public class App {
    private static final String LOG_PATH = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer" +
            "\\src\\main\\resources\\rkc-2.log";
    private static JavaSparkContext javaSparkContext;

    public static void main(String[] args) {
        javaSparkContext = initializeSpark();

        LogInsights insights = new LogInsights();
        insights.setLogLines(javaSparkContext.textFile(LOG_PATH));

        System.out.println("Total loglines: ");
        ArrayList<AccessLog> cleanData = new ArrayList<>(insights.getCleanedData());

        cleanData.removeIf(log -> log.getResponseCode() != 200 && log.getContentSize() == 0);


        System.out.println("Size of cleaned data set: " + cleanData.size());

        Clusters clusters = new Clusters(cleanData);
        clusters.calculateSimilarityVector();
        clusters.clusterKMeans(5);


        List<String> uniqueIps = insights.getDistictIpAddresses();
        System.out.println("Unique IPs: " + uniqueIps);

        List<String> ipAddressData = insights.getFrequentIpStatistics(50);
        insights.getFrequentIpStatistics(30);

        System.out.println("More than 20 times: " + ipAddressData);

        TreeMap<String, IpAddressGeoData> ipGeoData = insights.getIpGeoData();
        for (Map.Entry<String, IpAddressGeoData> ipAddress : ipGeoData.entrySet()) {
            System.out.println("IP: " + ipAddress.getValue().getIp() +
                    "; Geographical data: " + ipAddress.getValue().getCity() + ", " +
                    ipAddress.getValue().getCountryName());
        }

        System.out.println("Traffic statistics: " + insights.getTrafficStatistics());
        List<Tuple2<Integer, Long>> statusCodes = insights.getStatusCodeStatistics();
        List<StatusData> responseCodeData = new ArrayList<>();

        for (Tuple2<Integer, Long> element : statusCodes) {
            responseCodeData.add(new StatusData(element._1(), element._2()));
        }

        for (StatusData entry : responseCodeData) {
            System.out.println("Response code: " + entry.getStatusCode() + "\t count: " + entry.getCount());
        }


        System.out.println("Enter exit to exit: ");
        Scanner in = new Scanner(System.in);
        String command = in.next();

        if (command.equals("exit"))
            javaSparkContext.stop();
    }

    public static JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    /**
     * Converts selected resource file accessed by its URL to a String
     *
     * @deprecated
     */
    private static String readLog() {
        try {
            URL url = Resources.getResource("rkc-2.log");
            //url.toString();
            return Resources.toString(url, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Initializes Apache Spark for performing computations, sets basic properties of
     * the Spark application
     * @return {@link JavaSparkContext} object that is used to perform computation of log statistics
     * and for Data Mining tasks
     * */
    private static JavaSparkContext initializeSpark() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        return new JavaSparkContext(sparkConf);
    }
}
