package utils;

import main.App;
import org.apache.commons.cli.*;
import org.apache.spark.streaming.Duration;

/**
 * Created by maksym on 10.03.17.
 */
public class Flags {
    private static final Flags INSTANCE = new Flags();

    private boolean isHelp;
    private Duration windowLength;
    private Duration slideInterval;
    private String logsDirectory;
    private String outputHtmlFile;
    private String checkpointDirectory;

    private boolean initialized = false;

    public Flags() {
    }

    public boolean isHelp() {
        return isHelp;
    }

    public static Flags getInstance() {
        if (!INSTANCE.initialized) {
            throw new RuntimeException("Flags have not been initialized");
        }
        return INSTANCE;
    }

    public Duration getWindowLength() {
        return windowLength;
    }

    public String getLogsDirectory() {
        return logsDirectory;
    }

    public String getOutputHtmlFile() {
        return outputHtmlFile;
    }

    public String getCheckpointDirectory() {
        return checkpointDirectory;
    }

    public Duration getSlideInterval() {
        return slideInterval;
    }

    public static void setFromCommandLineArgs(Options options, String[] args) {
        CommandLineParser parser = new PosixParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            INSTANCE.isHelp = commandLine.hasOption(App.HELP);
            INSTANCE.windowLength = new Duration(Integer.parseInt(
                    commandLine.getOptionValue(App.WINDOW_LENGTH, "30")) * 1000);
            INSTANCE.slideInterval = new Duration(Integer.parseInt(
                    commandLine.getOptionValue(App.SLIDE_INTERVAL, "5")) * 1000);
            INSTANCE.logsDirectory = commandLine.getOptionValue(
                    App.LOGS_DIRECTORY, "/tmp/logs");
            INSTANCE.outputHtmlFile = commandLine.getOptionValue(
                    App.OUTPUT_HTML, "/output/log_stats.html");
            INSTANCE.checkpointDirectory = commandLine.getOptionValue(
                    App.CHECKPOINT_DIR, "/tmp/webloganalyzer-streaming");
            INSTANCE.initialized = true;
        } catch (ParseException e) {
            INSTANCE.initialized = false;
            System.err.println("Parsing failed. Error code: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
