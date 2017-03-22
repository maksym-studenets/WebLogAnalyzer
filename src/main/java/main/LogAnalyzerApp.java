package main;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import utils.Flags;

import java.io.IOException;
import java.net.URL;

/**
 * Created by maksym on 10.03.17.
 */
public class LogAnalyzerApp {

    public static final String HELP = "h";
    public static final String WINDOW_LENGTH = "w";
    public static final String SLIDE_INTERVAL = "s";
    public static final String LOGS_DIRECTORY = "l";
    public static final String OUTPUT_HTML = "o";
    public static final String CHECKPOINT_DIR = "c";

    private static final Options OPTIONS = createOptions();

    public static void main(String[] args) {
        String logFile = readLog();

        SparkConf conf = new SparkConf().setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
                Flags.getInstance().getSlideInterval());

        JavaRDD<String> logLines = javaSparkContext.textFile(logFile);
        System.out.println("Log lines: ");
        System.out.println(logLines);

        javaStreamingContext.checkpoint(Flags.getInstance().getCheckpointDirectory());


        javaSparkContext.stop();

    }

    private static Options createOptions() {
        Options options = new Options();
        options.addOption(LOGS_DIRECTORY, "logs-dir", true, "Log files directory");
        options.addOption(OUTPUT_HTML, "output-html", true, "Output statistics directory");
        options.addOption(WINDOW_LENGTH, "window-length", true, "Length of the window, seconds");
        options.addOption(SLIDE_INTERVAL, "slide-interval", true, "Slide interval, seconds");
        options.addOption(CHECKPOINT_DIR, "checkpoint-dir", true, "Spark checkpoints directory");
        options.addOption(HELP, "help", false, "Print help");

        return options;
    }

    private static void printAndQuit(int status) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Web Log Analyzer [args]: ", OPTIONS);
        System.exit(status);
    }

    private static String readLog() {
        try {
            //URL url = Resources.getResource("rkc.log");
            URL url = Resources.getResource("log.txt");
            url.toString();
            return Resources.toString(url, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
}
