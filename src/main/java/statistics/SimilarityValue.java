package statistics;

/**
 * Represents similarity value of two individual log lines
 */
public class SimilarityValue {
    private int row1;
    private int row2;
    private double value;

    public SimilarityValue(int row1, int row2, double value) {
        this.row1 = row1;
        this.row2 = row2;
        this.value = value;
    }

    public int getRow1() {
        return row1;
    }

    public void setRow1(int row1) {
        this.row1 = row1;
    }

    public int getRow2() {
        return row2;
    }

    public void setRow2(int row2) {
        this.row2 = row2;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}

