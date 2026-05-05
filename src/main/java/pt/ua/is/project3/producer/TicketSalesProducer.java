package pt.ua.is.project3.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pt.ua.is.project3.model.TicketSale;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TicketSalesProducer {

    private static final String TOPIC = "Proj3TicketSalesTopic";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Gson gson = new Gson();
        Random random = new Random();

        String[][] events = {
                {"E1", "Tech Conference 2026", "Aveiro", "Technology", "40.0"},
                {"E2", "Rock Festival", "Porto", "Music", "55.0"},
                {"E3", "Football Match", "Lisboa", "Sports", "30.0"},
                {"E4", "Comedy Night", "Coimbra", "Entertainment", "25.0"},
                {"E5", "Jazz Festival", "Braga", "Music", "35.0"}
        };

        String[] ticketTypes = {"NORMAL", "VIP", "STUDENT"};

        while (true) {
            String[] selected = events[random.nextInt(events.length)];

            String eventId = selected[0];
            String eventName = selected[1];
            String city = selected[2];
            String category = selected[3];
            double basePrice = Double.parseDouble(selected[4]);

            String ticketType = ticketTypes[random.nextInt(ticketTypes.length)];
            int quantity = random.nextInt(4) + 1;

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
}