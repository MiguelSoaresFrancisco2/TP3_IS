package pt.ua.is.project3.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pt.ua.is.project3.model.EventExpense;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class EventExpensesProducer {

    private static final String TOPIC = "Proj3EventExpensesTopic";

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

        EventInfo(String eventId, String eventName) {
            this.eventId = eventId;
            this.eventName = eventName;
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

        String[] expenseTypes = {"venue", "security", "marketing", "staff", "equipment"};

        System.out.println("Loaded " + events.size() + " events from database.");
        System.out.println("EventExpensesProducer started.");

        while (true) {
            EventInfo selected = events.get(random.nextInt(events.size()));

            String eventId = selected.eventId;
            String eventName = selected.eventName;
            String expenseType = expenseTypes[random.nextInt(expenseTypes.length)];

            double cost = 50 + random.nextInt(200);

            EventExpense expense = new EventExpense(
                    UUID.randomUUID().toString(),
                    eventId,
                    eventName,
                    expenseType,
                    cost
            );

            String json = gson.toJson(expense);

            producer.send(new ProducerRecord<>(TOPIC, eventId, json));

            System.out.println("EXPENSE -> " + json);

            Thread.sleep(2500);
        }
    }

    private static List<EventInfo> loadEventsFromDatabase() throws Exception {
        List<EventInfo> events = new ArrayList<>();

        String sql = """
                SELECT event_id, event_name
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
                        rs.getString("event_name")
                ));
            }
        }

        return events;
    }
}