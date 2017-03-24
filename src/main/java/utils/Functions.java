package utils;

import model.AccessLog;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Class {@code Functions} provides utility functions to filter by certain fields of the
 * Apache web server access logs, like filtering of IP addresses, by content size
 */
public class Functions {
    public static final Function2<Long, Long, Long> sumReducer = (a, b) -> a + b;

    public static Function2<List<Long>, Optional<Long>, Optional<Long>>
            calculateRunningSum = (nums, current) -> {
        long sum = current.or(0L);
        for (long i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };

    @Nullable
    public static Tuple4<Long, Long, Long, Long> generateContentSizeStats(
            JavaRDD<AccessLog> accessLogRDD) {
        JavaRDD<Long> contentSize = accessLogRDD.map(AccessLog::getContentSize).cache();
        long count = contentSize.count();
        if (count == 0) {
            return null;
        }
        return new Tuple4<>(count,
                contentSize.reduce(sumReducer),
                contentSize.min(Comparator.naturalOrder()),
                contentSize.max(Comparator.naturalOrder()));
    }

    public static JavaPairRDD<String, Long> countIpAddresses(
            JavaRDD<AccessLog> accessLogJavaRDD) {
        return accessLogJavaRDD
                .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                .reduceByKey(sumReducer);
    }

    public static JavaRDD<String> filterIpAddresses(
            JavaPairRDD<String, Long> ipAddressCount) {
        return ipAddressCount
                .filter(tuple -> tuple._2() >= 10)
                .map(Tuple2::_1);
    }

    public static JavaPairRDD<String, Long> calculateEndpointCount(
            JavaRDD<AccessLog> accessLogRDD) {
        return accessLogRDD
                .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
                .reduceByKey(sumReducer);
    }

    public static final class ValueComparator<K, V> implements
            Comparator<Tuple2<K, V>>, Serializable {
        private final Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
            return comparator.compare(o1._2(), o2._2());
        }
    }

    private Functions() {
    }
}
