package statistics;

import model.AccessLog;
import model.IpAddressGeoData;
import model.TrafficInfo;
import org.apache.spark.api.java.JavaRDD;
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
 * Performs basic log insight operationsreated by Maksym on 04.04.2017.
 */
public class LogInsights {
    private JavaRDD<AccessLog> accessLogs;
    private JavaRDD<Long> trafficData;
    private JavaRDD<String> logLines;
    private List<String> ipAddresses;


    public LogInsights() {
        accessLogs = null;
        trafficData = null;
        logLines = null;
    }


    public List<String> getIpStats(int quantity) {
        convertToAccessLog();
        try {
            ipAddresses =
                    accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                            .reduceByKey(Functions.sumReducer)
                            .filter(tuple -> tuple._2() > quantity)
                            .map(Tuple2::_1)
                            .take(100);
            //System.out.println(String.format("IPAddresses > " + quantity + "times: %s", ipAddresses));
            return ipAddresses;
        } catch (Exception e) {
            System.out.println("Something happened....");
            System.out.println("Stack trace: ");
            e.printStackTrace();
            return null;
        }
    }

    public TreeMap<String, IpAddressGeoData> getIpGeoData() {
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(IpGeoService.BASE_URL)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        IpGeoService ipGeoService = retrofit.create(IpGeoService.class);

        final TreeMap<String, IpAddressGeoData> ipGeoData = new TreeMap<>();

        for (String ipAddress : ipAddresses) {
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

    public TrafficInfo getTrafficStatistics() {
        trafficData = accessLogs.map(AccessLog::getContentSize).cache();
        long average = trafficData.reduce(Functions.sumReducer) / trafficData.count();
        long maximum = trafficData.max(Comparator.naturalOrder());
        long minimum = trafficData.min(Comparator.naturalOrder());

        return new TrafficInfo(average, maximum, minimum);
    }

    public List<Tuple2<Integer, Long>> getStatusCodeStatistics() {
        return accessLogs
                .mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
                .reduceByKey(Functions.sumReducer)
                .take(100);
    }

    public JavaRDD<String> getLogLines() {
        return logLines;
    }

    public void setLogLines(JavaRDD<String> logLines) {
        this.logLines = logLines;
    }

    private void convertToAccessLog() {
        accessLogs = logLines.map(AccessLog::parseLog).cache();
    }
}
