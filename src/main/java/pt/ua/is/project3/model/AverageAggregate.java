package pt.ua.is.project3.model;

public class AverageAggregate {

    public double total;
    public long count;

    public AverageAggregate() {
        this.total = 0.0;
        this.count = 0;
    }

    public AverageAggregate(double total, long count) {
        this.total = total;
        this.count = count;
    }

    public AverageAggregate add(double value) {
        this.total += value;
        this.count += 1;
        return this;
    }

    public double getAverage() {
        if (count == 0) {
            return 0.0;
        }

        return total / count;
    }
}