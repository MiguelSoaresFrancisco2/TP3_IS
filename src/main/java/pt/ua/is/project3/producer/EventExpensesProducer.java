package pt.ua.is.project3.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pt.ua.is.project3.model.EventExpense;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class EventExpensesProducer {

    private static final String TOPIC = "Proj3EventExpensesTopic";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Gson gson = new Gson();
        Random random = new Random();

        String[][] events = {
                {"E1", "Tech Conference 2026"},
                {"E2", "Rock Festival"},
                {"E3", "Football Match"},
                {"E4", "Comedy Night"},
                {"E5", "Jazz Festival"}
        };

        String[] expenseTypes = {"venue", "security", "marketing", "staff", "equipment"};

        while (true) {
            String[] selected = events[random.nextInt(events.length)];

            String eventId = selected[0];
            String eventName = selected[1];
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
}