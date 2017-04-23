package main;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import statistics.LogInsights;

/**
 * Main class of the application. Initializes JavaFX form
 */
public class App extends Application {
    private static final String LOG_PATH = "D:\\Progs\\JAVA\\2017\\2\\webloganalyzer" +
            "\\src\\main\\resources\\rkc-2.log";
    private static JavaSparkContext javaSparkContext;

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

    public static void main(String[] args) {
        Runnable runnable = () -> javaSparkContext = initializeSpark();
        runnable.run();

        launch(args);
        LogInsights insights = new LogInsights();
        insights.setLogLines(javaSparkContext.textFile(LOG_PATH));
    }

    public static JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    private static JavaSparkContext initializeSpark() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Analyzer")
                .setMaster("local[2]");
        return new JavaSparkContext(sparkConf);
    }
}
