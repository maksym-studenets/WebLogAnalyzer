package main;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import model.AccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.net.URL;
import java.util.Comparator;

/**
 * Created by maksym on 10.03.17.
 */
public class LogAnalyzerApp {

    public static final String HELP = "h";

    public static void main(String[] args) {
        String logFile = readLog();

        SparkConf conf = new SparkConf().setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> logLines = javaSparkContext.textFile(logFile);

        JavaRDD<AccessLog> accessLogs =
                logLines.map(AccessLog::parseLog).cache();
        /*
        JavaRDD<Long> contentSize =
                accessLogs.map(AccessLog::getContentSize).cache();
        System.out.println(String.format("Content size, average: %s,  Min: %s,  Max: %s",
                contentSize.reduce(SUM_REDUCER) / contentSize.count(),
                contentSize.min(Comparator.naturalOrder()),
                contentSize.max(Comparator.naturalOrder())));
                */

        JavaRDD<Long> contentSizes =
                accessLogs.map(AccessLog::getContentSize).cache();
        System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
                contentSizes.reduce(SUM_REDUCER) / contentSizes.count(),
                contentSizes.min(Comparator.naturalOrder()),
                contentSizes.max(Comparator.naturalOrder())));


        /*
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
                Flags.getInstance().getSlideInterval());

        javaStreamingContext.checkpoint(Flags.getInstance().getCheckpointDirectory());
        JavaDStream<String> logData = javaStreamingContext.textFileStream(Flags.getInstance()
                .getLogsDirectory());

        JavaDStream<AccessLog> accessLogDStream = logData.flatMap(
                line -> {
                    try {
                        return Collections.singleton(AccessLog.parseLog(line)).iterator();
                    } catch (IOException e) {
                        return Collections.emptyIterator();
                    }
                }).cache();

        JavaDStream<String> ipAddressDStream = accessLogDStream
                .transformToPair(Functions::ipAddressCount);
                */

        javaSparkContext.stop();

    }

    private static String readLog() {
        try {
            URL url = Resources.getResource("rkc.log");
            return Resources.toString(url, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
}
