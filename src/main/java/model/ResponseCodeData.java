package model;

/**
 * Represents response code data scheme
 */
public class ResponseCodeData {
    private int statusCode;
    private long count;

    public ResponseCodeData(int statusCode, long count) {
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
