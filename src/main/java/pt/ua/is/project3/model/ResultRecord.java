package pt.ua.is.project3.model;

public class ResultRecord {
    public String id;
    public String eventName;
    public double value;

    public ResultRecord(String id, String eventName, double value) {
        this.id = id;
        this.eventName = eventName;
        this.value = value;
    }
}