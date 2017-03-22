package model;

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by maksym on 10.03.17.
 */
public class AccessLog implements Serializable {
    private static final Logger logger = Logger.getLogger("Access");

    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    private String ipAddress;
    private String clientId;
    private String userID;
    private String dateTimeString;
    private String method;
    private String endpoint;
    private String protocol;
    private int responseCode;
    private long contentSize;

    /** Reads log file line by line and parses it. The result of a parse is a new object of AccessLog class.
     * @param logLine String that encompasses one line of Apache web server log
     * @return AccessLog object that represents a single line of a log
     * @throws IOException if matcher cannot find data
     * */
    public static AccessLog parseLog(String logLine) throws IOException {
        Matcher matcher = PATTERN.matcher(logLine);
        try {
            if (!matcher.find()) {
                logger.log(Level.ALL, "Error parsing line{0}: ", logLine);
                throw new IOException("Error parsing log line");
            }
        } catch (IOException e) {
            return null;
        }

        return new AccessLog(matcher.group(1), matcher.group(2), matcher.group(3),
                matcher.group(4), matcher.group(5), matcher.group(6),
                matcher.group(7), matcher.group(8), matcher.group(9));
    }

    private AccessLog(String ipAddress, String clientId, String userID, String dateTimeString,
                      String method, String endpoint, String protocol,
                      String responseCode, String contentSize) {
        this.ipAddress = ipAddress;
        this.clientId = clientId;
        this.userID = userID;
        this.dateTimeString = dateTimeString;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        if (contentSize.equals("-")) {
            this.contentSize = 0;
        } else {
            this.contentSize = Long.parseLong(contentSize);
        }
    }

    /** Returns IP address of a user
     * @return ipAddress
     * */
    public String getIpAddress() {
        return ipAddress;
    }

    /** Sets an IP address value from the parameter
     * @param ipAddress IP address to be set to the log access object
     * */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s",
                ipAddress, clientId, userID, dateTimeString, method, endpoint,
                protocol, responseCode, contentSize);
    }
}
