package pt.ua.is.project3.model;

public class EventExpense {
    public String expenseId;
    public String eventId;
    public String eventName;
    public String expenseType;
    public double cost;

    public EventExpense() {
    }

    public EventExpense(String expenseId, String eventId, String eventName,
                        String expenseType, double cost) {
        this.expenseId = expenseId;
        this.eventId = eventId;
        this.eventName = eventName;
        this.expenseType = expenseType;
        this.cost = cost;
    }
}