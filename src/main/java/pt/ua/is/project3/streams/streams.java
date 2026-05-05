package pt.ua.is.project3.streams;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import pt.ua.is.project3.model.EventExpense;
import pt.ua.is.project3.model.ResultRecord;
import pt.ua.is.project3.model.TicketSale;

import java.util.Properties;

public class streams {

    private static final String SALES_TOPIC = "Proj3TicketSalesTopic";
    private static final String EXPENSES_TOPIC = "Proj3EventExpensesTopic";

    private static final String REVENUE_PER_EVENT_TOPIC = "Proj3RevenuePerEventOutputStreamsTopic";
    private static final String EXPENSES_PER_EVENT_TOPIC = "Proj3ExpensesPerEventOutputStreamsTopic";
    private static final String PROFIT_PER_EVENT_TOPIC = "Proj3ProfitPerEventOutputStreamsTopic";

    public static void main(String[] args) {
        Gson gson = new Gson();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventtickets-streams-app-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> salesStream = builder.stream(
                SALES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> expensesStream = builder.stream(
                EXPENSES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KTable<String, Double> revenuePerEvent = salesStream
                .mapValues(value -> gson.fromJson(value, TicketSale.class))
                .groupBy((key, sale) -> sale.eventId, Grouped.with(Serdes.String(), null))
                .aggregate(
                        () -> 0.0,
                        (eventId, sale, total) -> total + sale.getRevenue(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        revenuePerEvent
                .toStream()
                .mapValues(value -> gson.toJson(new ResultRecord("REVENUE_" + System.currentTimeMillis(), "", value)))
                .to(REVENUE_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, Double> expensesPerEvent = expensesStream
                .mapValues(value -> gson.fromJson(value, EventExpense.class))
                .groupBy((key, expense) -> expense.eventId, Grouped.with(Serdes.String(), null))
                .aggregate(
                        () -> 0.0,
                        (eventId, expense, total) -> total + expense.cost,
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        expensesPerEvent
                .toStream()
                .mapValues(value -> gson.toJson(new ResultRecord("EXPENSES_" + System.currentTimeMillis(), "", value)))
                .to(EXPENSES_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, Double> profitPerEvent = revenuePerEvent.join(
                expensesPerEvent,
                (revenue, expenses) -> revenue - expenses
        );

        profitPerEvent
                .toStream()
                .mapValues(value -> gson.toJson(new ResultRecord("PROFIT_" + System.currentTimeMillis(), "", value)))
                .to(PROFIT_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();

        System.out.println("EventTickets Kafka Streams started.");
    }

}