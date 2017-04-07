package statistics;

import model.AccessLog;
import model.IpAddressGeoData;
import model.TrafficInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/**
 * Performs basic log insight operations. Uses for parsing and fetching Apache access log data,
 * perform clustering
 */
public class LogInsights {
    private JavaRDD<AccessLog> accessLogs;
    private JavaRDD<Long> trafficData;
    private JavaRDD<String> logLines;
    private List<String> frequentIpAddresses;


    /**
     * Default public constructor with no parameters. Sets references to class's fields to null
     */
    public LogInsights() {
        accessLogs = null;
        trafficData = null;
        logLines = null;
    }

    public List<AccessLog> getCleanedData() {
        return getLogList();
    }

    public List<String> getDistictIpAddresses() {
        //convertToAccessLog();
        try {
            return accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                    .reduceByKey(Functions.sumReducer)
                    .distinct()
                    .map(Tuple2::_1)
                    .take(100);
        } catch (Exception e) {
            System.out.println("Something happened....");
            System.out.println("Stack trace: ");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Retrieves a list of IP addresses that accessed the website with a specified number of times
     * or more often. Before conducting basic operations, parses log data line by line using
     * Spark's built-in capabilities
     *
     * @param quantity filter for the IP address list. All addresses that occur more often than this value
     *                 will be added to the list
     * @return list of IP addresses that occur more than a value passed as a parameter
     */
    public List<String> getFrequentIpStatistics(int quantity) {
        //convertToAccessLog();
        try {
            frequentIpAddresses =
                    accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                            .reduceByKey(Functions.sumReducer)
                            .filter(tuple -> tuple._2() > quantity)
                            .map(Tuple2::_1)
                            .take(100);
            //System.out.println(String.format("IPAddresses > " + quantity + "times: %s", frequentIpAddresses));
            return frequentIpAddresses;
        } catch (Exception e) {
            System.out.println("Something happened....");
            System.out.println("Stack trace: ");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Retrieves geographical information for the selected list of IP addresses that occur
     * in the web log more frequently than in the retrieving method. Sends REST API request
     * to FreeGeoIp.net service and receives a response. Uses Retrofit library to safely process
     * HTTP request. {@link GsonConverterFactory} is used to parse the response to the desired
     * custom type.
     * @return {@link TreeMap} of IP addresses and appropriate set of geo data
     * */
    public TreeMap<String, IpAddressGeoData> getIpGeoData() {
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(IpGeoService.BASE_URL)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        IpGeoService ipGeoService = retrofit.create(IpGeoService.class);

        final TreeMap<String, IpAddressGeoData> ipGeoData = new TreeMap<>();

        for (String ipAddress : frequentIpAddresses) {
            ipGeoService.getIpAddressGeoData(ipAddress).enqueue(
                    new Callback<IpAddressGeoData>() {
                        @Override
                        public void onResponse(Call<IpAddressGeoData> call,
                                               Response<IpAddressGeoData> response) {
                            IpAddressGeoData data = response.body();
                            ipGeoData.put(ipAddress, data);
                        }

                        @Override
                        public void onFailure(Call<IpAddressGeoData> call, Throwable throwable) {
                            System.out.println("Something went wrong while fetching geo data");
                        }
                    });
        }
        return ipGeoData;
    }

    /**
     * Computes traffic data statistics for the complete dataset of web logs.
     * Basic statistical information includes minimum, average and maximum content size
     * of each request.
     * @return new object of {@link TrafficInfo} class that represents computed data. It is
     * then being used to present in a easy to use form
     * */
    public TrafficInfo getTrafficStatistics() {
        trafficData = accessLogs.map(AccessLog::getContentSize).cache();
        trafficData.filter((Function<Long, Boolean>) v1 -> v1 > 0);
        long average = trafficData.reduce(Functions.sumReducer) / trafficData.count();
        long maximum = trafficData.max(Comparator.naturalOrder());
        long minimum = trafficData.min(Comparator.naturalOrder());

        return new TrafficInfo(average, maximum, minimum);
    }

    /**
     * Returns list of status code statistics
     * */
    public List<Tuple2<Integer, Long>> getStatusCodeStatistics() {
        return accessLogs
                .mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
                .reduceByKey(Functions.sumReducer)
                .take(100);
    }


    /*
    public JavaRDD<AccessLog> getTimeDataForIp() {
        return accessLogs
                .mapToPair(log -> new Tuple2<>(log.getDate(), 1L))
                .reduceByKey(Functions.sumReducer)
                .filter(date -> );
    }
    */


    public void setLogLines(JavaRDD<String> logLines) {
        this.logLines = logLines;
        convertToAccessLog();
    }

    private List<AccessLog> getLogList() {
        return accessLogs.collect();
    }

    /***/
    private void convertToAccessLog() {
        accessLogs = logLines.map(AccessLog::parseLog).cache();
    }
}
