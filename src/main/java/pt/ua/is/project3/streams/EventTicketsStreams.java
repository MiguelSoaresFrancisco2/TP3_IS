package pt.ua.is.project3.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import pt.ua.is.project3.model.EventExpense;
import pt.ua.is.project3.model.ResultRecord;
import pt.ua.is.project3.model.TicketSale;
import pt.ua.is.project3.serde.JsonSerde;

import java.util.Properties;

public class EventTicketsStreams {

    private static final String SALES_TOPIC = "Proj3TicketSalesTopic";
    private static final String EXPENSES_TOPIC = "Proj3EventExpensesTopic";

    private static final String REVENUE_PER_EVENT_TOPIC = "Proj3RevenuePerEventOutputStreamsTopic";
    private static final String EXPENSES_PER_EVENT_TOPIC = "Proj3ExpensesPerEventOutputStreamsTopic";
    private static final String PROFIT_PER_EVENT_TOPIC = "Proj3ProfitPerEventOutputStreamsTopic";

    private static final String TOTAL_REVENUE_TOPIC = "Proj3TotalRevenueOutputStreamsTopic";
    private static final String TOTAL_EXPENSES_TOPIC = "Proj3TotalExpensesOutputStreamsTopic";
    private static final String TOTAL_PROFIT_TOPIC = "Proj3TotalProfitOutputStreamsTopic";

    private static final String TOTAL_KEY = "TOTAL";

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    private static String toConnectJson(ResultRecord record) {
        return "{"
                + "\"schema\":{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},"
                + "{\"type\":\"string\",\"optional\":true,\"field\":\"eventName\"},"
                + "{\"type\":\"double\",\"optional\":false,\"field\":\"value\"}"
                + "],"
                + "\"optional\":false,"
                + "\"name\":\"ResultRecord\""
                + "},"
                + "\"payload\":{"
                + "\"id\":\"" + escapeJson(record.id) + "\","
                + "\"eventName\":\"" + escapeJson(record.eventName) + "\","
                + "\"value\":" + record.value
                + "}"
                + "}";
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventtickets-streams-app-v4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<TicketSale> ticketSaleSerde = new JsonSerde<>(TicketSale.class);
        JsonSerde<EventExpense> eventExpenseSerde = new JsonSerde<>(EventExpense.class);
        JsonSerde<ResultRecord> resultRecordSerde = new JsonSerde<>(ResultRecord.class);

        KStream<String, TicketSale> salesStream = builder.stream(
                SALES_TOPIC,
                Consumed.with(Serdes.String(), ticketSaleSerde)
        );

        KStream<String, EventExpense> expensesStream = builder.stream(
                EXPENSES_TOPIC,
                Consumed.with(Serdes.String(), eventExpenseSerde)
        );

        /*
         * Requirement 5:
         * Revenue per event.
         * Solved with reduce().
         */
        KTable<String, ResultRecord> revenuePerEvent = salesStream
                .mapValues(sale -> new ResultRecord(
                        sale.eventId,
                        sale.eventName,
                        sale.getRevenue()
                ))
                .groupByKey(Grouped.with(Serdes.String(), resultRecordSerde))
                .reduce((oldValue, newValue) -> new ResultRecord(
                        oldValue.id,
                        oldValue.eventName,
                        oldValue.value + newValue.value
                ));

        revenuePerEvent
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(REVENUE_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Requirement 6:
         * Expenses per event.
         * Solved with reduce().
         */
        KTable<String, ResultRecord> expensesPerEvent = expensesStream
                .mapValues(expense -> new ResultRecord(
                        expense.eventId,
                        expense.eventName,
                        expense.cost
                ))
                .groupByKey(Grouped.with(Serdes.String(), resultRecordSerde))
                .reduce((oldValue, newValue) -> new ResultRecord(
                        oldValue.id,
                        oldValue.eventName,
                        oldValue.value + newValue.value
                ));

        expensesPerEvent
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(EXPENSES_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Requirement 7:
         * Profit per event.
         * Solved with join().
         */
        KTable<String, ResultRecord> profitPerEvent = revenuePerEvent.join(
                expensesPerEvent,
                (revenue, expenses) -> new ResultRecord(
                        revenue.id,
                        revenue.eventName,
                        revenue.value - expenses.value
                )
        );

        profitPerEvent
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(PROFIT_PER_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Requirement 8:
         * Total revenue.
         * Solved directly from ticket sales with reduce().
         *
         * Important:
         * The Kafka Streams key is TOTAL_KEY so that it can join with totalExpenses.
         */
        KTable<String, ResultRecord> totalRevenue = salesStream
                .map((key, sale) -> KeyValue.pair(
                        TOTAL_KEY,
                        new ResultRecord(
                                "TOTAL_REVENUE",
                                "Total Revenue",
                                sale.getRevenue()
                        )
                ))
                .groupByKey(Grouped.with(Serdes.String(), resultRecordSerde))
                .reduce((oldValue, newValue) -> new ResultRecord(
                        "TOTAL_REVENUE",
                        "Total Revenue",
                        oldValue.value + newValue.value
                ));

        totalRevenue
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(TOTAL_REVENUE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Requirement 9:
         * Total expenses.
         * Solved directly from event expenses with reduce().
         *
         * Important:
         * The Kafka Streams key is also TOTAL_KEY so that it can join with totalRevenue.
         */
        KTable<String, ResultRecord> totalExpenses = expensesStream
                .map((key, expense) -> KeyValue.pair(
                        TOTAL_KEY,
                        new ResultRecord(
                                "TOTAL_EXPENSES",
                                "Total Expenses",
                                expense.cost
                        )
                ))
                .groupByKey(Grouped.with(Serdes.String(), resultRecordSerde))
                .reduce((oldValue, newValue) -> new ResultRecord(
                        "TOTAL_EXPENSES",
                        "Total Expenses",
                        oldValue.value + newValue.value
                ));

        totalExpenses
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(TOTAL_EXPENSES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Requirement 10:
         * Total profit.
         * Solved with join().
         *
         * This works because totalRevenue and totalExpenses now use the same key: TOTAL_KEY.
         */
        KTable<String, ResultRecord> totalProfit = totalRevenue.join(
                totalExpenses,
                (revenue, expenses) -> new ResultRecord(
                        "TOTAL_PROFIT",
                        "Total Profit",
                        revenue.value - expenses.value
                )
        );

        totalProfit
                .toStream()
                .mapValues(EventTicketsStreams::toConnectJson)
                .to(TOTAL_PROFIT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();

        System.out.println("EventTickets Kafka Streams started.");
        System.out.println("Calculating revenue, expenses and profit per event...");
        System.out.println("Calculating total revenue, total expenses and total profit...");
    }
}