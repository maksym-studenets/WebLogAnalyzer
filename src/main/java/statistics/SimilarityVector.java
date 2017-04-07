package statistics;

/**
 * Represents similarity value of two individual log lines
 */
public class SimilarityVector {
    private int log1;
    private int log2;
    private double similarity;

    public SimilarityVector(int log1, int log2, double similarity) {
        this.log1 = log1;
        this.log2 = log2;
        this.similarity = similarity;
    }

    @Override
    public String toString() {
        return "(" + log1 + ", " + log2 + "): " + similarity;
    }
}

