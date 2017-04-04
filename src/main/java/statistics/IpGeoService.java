package statistics;

import model.IpAddressGeoData;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

/**
 * Interface for Retrofit library to obtain IP address geographic data from the FreeGeoIp REST API service.
 */
public interface IpGeoService {
    /**
     * Represents base URL for the FreeGeoIp REST service
     */
    String BASE_URL = "http://freegeoip.net";

    /**
     * Retrofit model model to obtain geographical data from the API
     */
    @GET("/json")
    Call<IpAddressGeoData> getIpAddressGeoData(@Query("") String ipAddress);
}
