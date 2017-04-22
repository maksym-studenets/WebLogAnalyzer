package model;

/**
 * Represents basic Traffic Data.
 * This class is deprecated. Use {@link TrafficData} instead.
 * @see TrafficData
 */
@Deprecated
public class TrafficInfo {
    private long average;
    private long maximum;
    private long minimum;

    public TrafficInfo(long average, long maximum, long minimum) {
        this.average = average;
        this.maximum = maximum;
        this.minimum = minimum;
    }

    @Override
    public String toString() {
        return "Minimum: " + minimum + "; Average: " + average + "; Maximum: " + maximum;
    }
}
