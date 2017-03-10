import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import model.AccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

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
                }
        ).cache();

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
}
