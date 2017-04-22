package ui;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.FileChooser;
import main.App;
import model.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import statistics.Clusters;
import statistics.LogInsights;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

/**
 * Controller class for the Main JavaFX form. Handles events on the form and is used
 * for method invocation and feature presentation
 */
public class Controller implements Initializable {

    private Clusters clusters;
    private File logFile;
    private JavaSparkContext javaSparkContext;
    private LogInsights logInsights = new LogInsights();

    @FXML
    private Label logFilePathLabel;
    @FXML
    private TextArea logFileTextArea;
    @FXML
    private ListView<String> uniqueIpList;
    @FXML
    private ListView<String> frequentIpList;
    @FXML
    private TableView<TrafficData> trafficDataTable;
    @FXML
    private TableColumn<TrafficData, String> propertyColumn;
    @FXML
    private TableColumn<Object, Object> volumeColumn;
    @FXML
    private TableView<ResponseCodeData> responseCodeTableView;
    @FXML
    private TableColumn<ResponseCodeData, Integer> responseCodeColumn;
    @FXML
    private TableColumn<ResponseCodeData, Long> countColumn;

    @FXML
    private TextField clusterCountTextField;
    @FXML
    private TextField iterationCountTextField;
    @FXML
    private TextField kMeansComputeText;
    @FXML
    private CheckBox kMeansRecalculateCheckbox;
    @FXML
    private TableView<CentroidVector> kMeansAllCentroidsTable;
    @FXML
    private TableColumn<CentroidVector, Integer> kMeansAllClusterNoColumn;
    @FXML
    private TableColumn<CentroidVector, Vector> kMeansAllCentroidsColumn;
    @FXML
    private TableView<CentroidValue> kMeansLastCentroidTable;
    @FXML
    private TableColumn<CentroidValue, Integer> kMeansLastClusterNoColumn;
    @FXML
    private TableColumn<CentroidValue, Double> kMeansLastCentroidColumn;

    @FXML
    private TextField bisectingClusterCountText;
    @FXML
    private TextField bisectingComputeCostText;
    @FXML
    private CheckBox bisectingRecalculateCheckBox;
    @FXML
    private TableView<CentroidVector> bisectingCentroidsTable;
    @FXML
    private TableColumn<CentroidVector, Integer> clusterNumberBisectingColumn;
    @FXML
    private TableColumn<CentroidVector, Vector> centroidsBisectingColumn;
    @FXML
    private TableView<CentroidValue> bisectingLastCentroidTable;
    @FXML
    private TableColumn<CentroidValue, Integer> bisectingLastClusterNumberColumn;
    @FXML
    private TableColumn<CentroidValue, Vector> bisectingLastPointColumn;


    /**
     * Invoked on the form initialization. Initializes {@link JavaSparkContext} object
     * for the current project
     */
    @Override
    public void initialize(URL location, ResourceBundle resources) {
        this.javaSparkContext = App.getJavaSparkContext();
    }

    /**
     * Performs loading of a specified log file. The file is chosen using a {@link FileChooser} object.
     * The application is able to read and parse log data from files with any extension but for the
     * purposes of uniformity and consistency only *.log, *.txt or *.data files might be used.
     * Sets the value for a label corresponding to absolute file path of the chosen log file.
     */
    @FXML
    private void openFile(ActionEvent event) {
        Node node = (Node) event.getSource();
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open Log File");
        fileChooser.setInitialDirectory(new
                File("D:\\Progs\\JAVA\\2017\\2\\webloganalyzer\\src\\main\\resources")
        );
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("Log files", "*.log"),
                new FileChooser.ExtensionFilter("Text files", "*.txt"),
                new FileChooser.ExtensionFilter("Data files", "*.data"));
        logFile = fileChooser.showOpenDialog(node.getScene().getWindow());
        logFilePathLabel.setText(logFile.getAbsolutePath());

        Runnable runnable = () -> {
            logInsights.setLogLines(javaSparkContext.textFile(logFile.getAbsolutePath()));

            JavaRDD<String> logLines = logInsights.getLogLines();
            List<String> logs = logLines.collect();
            StringBuilder value = new StringBuilder();
            for (String log : logs) {
                value.append(log).append("\n");
            }

            logFileTextArea.setText(value.toString());
        };
        runnable.run();
    }

    /**
     * Computes general statistics of the loaded log file: unique IP addresses, frequent IP addresses
     * (those that appear more often than 20 times), minimum, average and maximum size of the content
     * transferred and statistics of response code occurrences. Statistics is presented in a easy to
     * understand visual form.
     */
    @FXML
    private void processLogFile() {
        List<String> uniqueIps = logInsights.getDistictIpAddresses();
        uniqueIpList.setItems(FXCollections.observableList(uniqueIps));
        List<String> frequentIps = logInsights.getFrequentIpStatistics(30);
        frequentIpList.setItems(FXCollections.observableList(frequentIps));

        List<TrafficData> trafficDataList = logInsights.getTrafficData();
        ObservableList<TrafficData> trafficData = FXCollections.observableList(trafficDataList);
        trafficDataTable.setItems(trafficData);
        propertyColumn.setCellValueFactory(new PropertyValueFactory<>("label"));
        volumeColumn.setCellValueFactory(new PropertyValueFactory<>("value"));

        List<Tuple2<Integer, Long>> statusCodes = logInsights.getStatusCodeStatistics();
        List<ResponseCodeData> responseCodeData = new ArrayList<>();
        for (Tuple2<Integer, Long> element : statusCodes) {
            responseCodeData.add(new ResponseCodeData(element._1(), element._2()));
        }
        ObservableList<ResponseCodeData> statusData =
                FXCollections.observableList(responseCodeData);
        responseCodeTableView.setItems(statusData);
        responseCodeColumn.setCellValueFactory(new PropertyValueFactory<>("statusCode"));
        countColumn.setCellValueFactory(new PropertyValueFactory<>("count"));
    }


    /**
     * Invokes k-means clustering method from the {@link Clusters} class. Displays centroid vectors
     * as computed by the Spark platform and the values of centroids at last iteration for each cluster.
     * Invokes similarity matrix recalculation method if the appropriate checkbox is checked. Otherwise,
     * the old version of that file will be used.
     *
     * @author Maksym
     * @see Clusters
     */
    @FXML
    private void clusterKMeans() {
        if (logFile != null) {
            int clusterCount = Integer.parseInt(clusterCountTextField.getText());
            int iterationCount = Integer.parseInt(iterationCountTextField.getText());

            ArrayList<AccessLog> cleanData = new ArrayList<>(logInsights.getCleanedData());
            cleanData.removeIf(log -> log.getResponseCode() != 200 &&
                    log.getContentSize() == 0);

            clusters = new Clusters(cleanData);

            if (kMeansRecalculateCheckbox.isSelected()) {
                Runnable calculateSimilarityRunnable = () -> clusters.calculateSimilarityMatrix();
                calculateSimilarityRunnable.run();
            }

            Runnable clusterKMeansRunnable = () -> clusters.clusterKMeans(clusterCount,
                    iterationCount);
            clusterKMeansRunnable.run();
            kMeansComputeText.setText(String.valueOf(clusters.getkMeansCost()));

            ArrayList<Vector> centroids = clusters.getkMeansCentroids();
            ArrayList<CentroidVector> centroidVectors = new ArrayList<>();
            ArrayList<CentroidValue> lastVectors = new ArrayList<>();
            double[] centroidValues;
            for (int i = 0; i < centroids.size(); i++) {
                centroidVectors.add(new CentroidVector(i + 1, centroids.get(i)));
                centroidValues = centroids.get(i).toArray();
                lastVectors.add(new CentroidValue(i + 1,
                        centroidValues[centroidValues.length - 1]));
            }

            ObservableList<CentroidVector> centroidsObservable = FXCollections
                    .observableList(centroidVectors);
            kMeansAllCentroidsTable.setItems(centroidsObservable);
            kMeansAllClusterNoColumn.setCellValueFactory(new PropertyValueFactory<>("clusterNo"));
            kMeansAllCentroidsColumn.setCellValueFactory(new PropertyValueFactory<>("points"));

            ObservableList<CentroidValue> lastCentroids = FXCollections
                    .observableList(lastVectors);
            kMeansLastCentroidTable.setItems(lastCentroids);
            kMeansLastClusterNoColumn.setCellValueFactory(new PropertyValueFactory<>("clusterNo"));
            kMeansLastCentroidColumn.setCellValueFactory(new PropertyValueFactory<>("centroid"));
        } else {
            showMissingFileDialog();
        }
    }

    /**
     * Invokes bisecting k-means clustering method from the {@link Clusters} class. Displays centroid vectors
     * as computed by the Spark platform and the values of centroids at last iteration for each cluster.
     * Invokes similarity matrix recalculation method if the appropriate checkbox is checked. Otherwise,
     * the old version of that file will be used.
     *
     * @author Maksym
     * @see Clusters
     */
    @FXML
    private void clusterBisectingKMeans() {
        if (logFile != null) {
            int clusterCount = Integer.parseInt(bisectingClusterCountText.getText());

            ArrayList<AccessLog> cleanData = new ArrayList<>(logInsights.getCleanedData());
            cleanData.removeIf(log -> log.getResponseCode() != 200 &&
                    log.getContentSize() == 0);

            clusters = new Clusters(cleanData);

            if (bisectingRecalculateCheckBox.isSelected()) {
                Runnable calculateSimilarityRunnable = () -> clusters.calculateSimilarityMatrix();
                calculateSimilarityRunnable.run();
            }

            Runnable clusterBisectingRunnable = () -> clusters.clusterBisectingKMeans(clusterCount);
            clusterBisectingRunnable.run();

            bisectingComputeCostText.setText(String.valueOf(clusters.getBisectingKMeansCost()));

            ArrayList<Vector> centroids = clusters.getBisectingKMeansCentroids();
            ArrayList<CentroidVector> centroidVectors = new ArrayList<>();
            ArrayList<CentroidValue> lastVectors = new ArrayList<>();
            double[] centroidValues;
            for (int i = 0; i < centroids.size(); i++) {
                centroidVectors.add(new CentroidVector(i + 1, centroids.get(i)));
                centroidValues = centroids.get(i).toArray();
                lastVectors.add(new CentroidValue(i + 1,
                        centroidValues[centroidValues.length - 1]));
            }

            ObservableList<CentroidVector> centroidsObservable = FXCollections
                    .observableList(centroidVectors);
            bisectingCentroidsTable.setItems(centroidsObservable);
            clusterNumberBisectingColumn.setCellValueFactory(new PropertyValueFactory<>("clusterNo"));
            centroidsBisectingColumn.setCellValueFactory(new PropertyValueFactory<>("points"));
            ObservableList<CentroidValue> lastCentroids = FXCollections
                    .observableList(lastVectors);
            bisectingLastCentroidTable.setItems(lastCentroids);
            bisectingLastClusterNumberColumn.setCellValueFactory(new PropertyValueFactory<>("clusterNo"));
            bisectingLastPointColumn.setCellValueFactory(new PropertyValueFactory<>("centroid"));
        } else {
            showMissingFileDialog();
        }
    }

    /**
     * Shows alert dialog if no file with log data was loaded
     */
    private void showMissingFileDialog() {
        Alert alert = new Alert(Alert.AlertType.WARNING);
        alert.setTitle("Warning");
        alert.setHeaderText("No log file loaded");
        alert.setContentText("Please load the log file in the general tab to continue!");
        alert.showAndWait();
    }
}