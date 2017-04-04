package model;

/**
 * Created by Maksym on 04.04.2017.
 */
public class StatusData {
    private int statusCode;
    private long count;

    public StatusData() {
    }

    public StatusData(int statusCode, long count) {
        this.statusCode = statusCode;
        this.count = count;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
