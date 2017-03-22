package utils;

import main.LogAnalyzerApp;
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
            throw new RuntimeException("utils.Flags have not been initialized");
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
            INSTANCE.isHelp = commandLine.hasOption(LogAnalyzerApp.HELP);
            INSTANCE.windowLength = new Duration(Integer.parseInt(
                    commandLine.getOptionValue(LogAnalyzerApp.WINDOW_LENGTH, "30")) * 1000);
            INSTANCE.slideInterval = new Duration(Integer.parseInt(
                    commandLine.getOptionValue(LogAnalyzerApp.SLIDE_INTERVAL, "5")) * 1000);
            INSTANCE.logsDirectory = commandLine.getOptionValue(
                    LogAnalyzerApp.LOGS_DIRECTORY, "/tmp/logs");
            INSTANCE.outputHtmlFile = commandLine.getOptionValue(
                    LogAnalyzerApp.OUTPUT_HTML, "/output/log_stats.html");
            INSTANCE.checkpointDirectory = commandLine.getOptionValue(
                    LogAnalyzerApp.CHECKPOINT_DIR, "/tmp/webloganalyzer-streaming");
            INSTANCE.initialized = true;
        } catch (ParseException e) {
            INSTANCE.initialized = false;
            System.err.println("Parsing failed. Error code: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
