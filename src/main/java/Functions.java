import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by maksym on 10.03.17.
 */
public class Functions {
    public static final Function2<Long, Long, Long> sumReducer = (a, b) -> a + b;

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
}
