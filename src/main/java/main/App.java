package main;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import model.StatusData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import statistics.LogInsights;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class for the Web Log Analyzer project
 */

/* TODO: ENABLE JAVAFX INTEGRATION WITH GRADLE. COMMENTED BELOW
 */

public class App  /* extends Application */ {
    private static JavaSparkContext javaSparkContext;
    private static final String LOG_PATH = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer" +
            "\\src\\main\\resources\\rkc.log";

    /*
    * @Override
    * Parent root = FXMLLoader.load(getClass().getResource("fxml/MainForm.fxml"));
        primaryStage.setTitle("Hello World");
        primaryStage.setScene(new Scene(root, 300, 275));
        primaryStage.show();
    * */


    public static void main(String[] args) {
        //launch(args);

        javaSparkContext = initializeSpark();
        LogInsights insights = new LogInsights();
        insights.setLogLines(javaSparkContext.textFile(LOG_PATH));
        List<String> ipAddressData = insights.getIpStats(50);
        insights.getIpStats(30);

        System.out.println("More than 20 times: " + ipAddressData);

        /*
        TreeMap<String, IpAddressGeoData> ipGeoData = insights.getIpGeoData();
        for (Map.Entry<String, IpAddressGeoData> ipAddress : ipGeoData.entrySet()) {
            System.out.println("IP: " + ipAddress.getValue().getIp() +
                    "; Geographical data: " + ipAddress.getValue().getCity() + ", " +
                    ipAddress.getValue().getCountryName());
        }
        */

        System.out.println("Traffic statistics: " + insights.getTrafficStatistics());
        List<Tuple2<Integer, Long>> statusCodes = insights.getStatusCodeStatistics();
        List<StatusData> responseCodeData = new ArrayList<>();

        for (Tuple2<Integer, Long> element : statusCodes) {
            responseCodeData.add(new StatusData(element._1(), element._2()));
        }

        for (StatusData entry : responseCodeData) {
            System.out.println("Response code: " + entry.getStatusCode() + "\t count: " + entry.getCount());
        }

        javaSparkContext.stop();
    }

    private static String readLog() {
        try {
            URL url = Resources.getResource("rkc.log");
            //url.toString();
            return Resources.toString(url, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static JavaSparkContext initializeSpark() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        return new JavaSparkContext(sparkConf);
    }
}
