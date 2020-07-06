package simpledb;

public interface Histogram {
    void addValue(Object v);
    double estimateSelectivity(Predicate.Op op, Object v);
}
