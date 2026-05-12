package pt.ua.is.project3.model;

public class TicketSale {
    public String saleId;
    public String eventId;
    public String eventName;
    public String city;
    public String category;
    public String ticketType;
    public double ticketPrice;
    public int quantity;

    public TicketSale() {
    }

    public TicketSale(String saleId, String eventId, String eventName, String city,
                      String category, String ticketType, double ticketPrice, int quantity) {
        this.saleId = saleId;
        this.eventId = eventId;
        this.eventName = eventName;
        this.city = city;
        this.category = category;
        this.ticketType = ticketType;
        this.ticketPrice = ticketPrice;
        this.quantity = quantity;
    }

    public double getRevenue() {
        return ticketPrice * quantity;
    }
}