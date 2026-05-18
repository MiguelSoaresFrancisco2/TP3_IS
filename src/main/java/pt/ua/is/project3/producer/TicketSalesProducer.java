package pt.ua.is.project3.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pt.ua.is.project3.model.TicketSale;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TicketSalesProducer {

    private static final String TOPIC = "Proj3TicketSalesTopic";
    private static final long RELOAD_INTERVAL_MS = 30_000; // 30 segundos

    private static final String DB_URL =
            System.getenv().getOrDefault(
                    "DB_URL",
                    "jdbc:postgresql://database:5432/project3"
            );

    private static final String DB_USER =
            System.getenv().getOrDefault("DB_USER", "postgres");

    private static final String DB_PASSWORD =
            System.getenv().getOrDefault("DB_PASSWORD", "nopass");

    private static class EventInfo {
        String eventId;
        String eventName;
        String city;
        String category;
        double baseTicketPrice;

        EventInfo(String eventId, String eventName, String city, String category, double baseTicketPrice) {
            this.eventId = eventId;
            this.eventName = eventName;
            this.city = city;
            this.category = category;
            this.baseTicketPrice = baseTicketPrice;
        }
    }

    public static void main(String[] args) throws Exception {
        List<EventInfo> events = loadEventsFromDatabase();

        if (events.isEmpty()) {
            System.err.println("No events found in database. Producer will stop.");
            return;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Gson gson = new Gson();
        Random random = new Random();

        String[] ticketTypes = {"NORMAL", "VIP", "STUDENT"};

        long lastReloadTime = System.currentTimeMillis();

        System.out.println("Loaded " + events.size() + " events from database.");
        System.out.println("TicketSalesProducer started.");

        while (true) {
            long now = System.currentTimeMillis();

            if (now - lastReloadTime >= RELOAD_INTERVAL_MS) {
                List<EventInfo> updatedEvents = loadEventsFromDatabase();

                if (!updatedEvents.isEmpty()) {
                    events = updatedEvents;
                    System.out.println("Reloaded " + events.size() + " events from database.");
                } else {
                    System.out.println("Reload skipped: database returned no events.");
                }

                lastReloadTime = now;
            }

            EventInfo selected = events.get(random.nextInt(events.size()));

            String eventId = selected.eventId;
            String eventName = selected.eventName;
            String city = selected.city;
            String category = selected.category;
            double basePrice = selected.baseTicketPrice;

            String ticketType = ticketTypes[random.nextInt(ticketTypes.length)];
            int quantity = random.nextInt(8) + 3;

            double multiplier;

            switch (ticketType) {
                case "VIP":
                    multiplier = 1.8;
                    break;
                case "STUDENT":
                    multiplier = 0.7;
                    break;
                default:
                    multiplier = 1.0;
                    break;
            }

            double ticketPrice = basePrice * multiplier;

            TicketSale sale = new TicketSale(
                    UUID.randomUUID().toString(),
                    eventId,
                    eventName,
                    city,
                    category,
                    ticketType,
                    ticketPrice,
                    quantity
            );

            String json = gson.toJson(sale);

            producer.send(new ProducerRecord<>(TOPIC, eventId, json));

            System.out.println("SALE -> " + json);

            Thread.sleep(1000);
        }
    }

    private static List<EventInfo> loadEventsFromDatabase() throws Exception {
        List<EventInfo> events = new ArrayList<>();

        String sql = """
                SELECT event_id, event_name, city, category, base_ticket_price
                FROM events
                ORDER BY event_id
                """;

        try (
                Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()
        ) {
            while (rs.next()) {
                events.add(new EventInfo(
                        rs.getString("event_id"),
                        rs.getString("event_name"),
                        rs.getString("city"),
                        rs.getString("category"),
                        rs.getBigDecimal("base_ticket_price").doubleValue()
                ));
            }
        }

        return events;
    }
}