package pt.ua.is.project3.api;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class EventTicketsRestApi {

    private static final int PORT = 8080;

    private static final String DB_URL =
            System.getenv().getOrDefault(
                    "DB_URL",
                    "jdbc:postgresql://localhost:5432/project3"
            );

    private static final String DB_USER =
            System.getenv().getOrDefault("DB_USER", "postgres");

    private static final String DB_PASSWORD =
            System.getenv().getOrDefault("DB_PASSWORD", "nopass");

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);

        server.createContext("/health", EventTicketsRestApi::handleHealth);

        server.createContext("/cities", EventTicketsRestApi::handleCities);
        server.createContext("/events", EventTicketsRestApi::handleEvents);

        server.createContext("/stats/revenue-per-event",
                exchange -> handleStats(exchange, "Proj3RevenuePerEventOutputStreamsTopic"));

        server.createContext("/stats/expenses-per-event",
                exchange -> handleStats(exchange, "Proj3ExpensesPerEventOutputStreamsTopic"));

        server.createContext("/stats/profit-per-event",
                exchange -> handleStats(exchange, "Proj3ProfitPerEventOutputStreamsTopic"));

        server.createContext("/stats/total-revenue",
                exchange -> handleStats(exchange, "Proj3TotalRevenueOutputStreamsTopic"));

        server.createContext("/stats/total-expenses",
                exchange -> handleStats(exchange, "Proj3TotalExpensesOutputStreamsTopic"));

        server.createContext("/stats/total-profit",
                exchange -> handleStats(exchange, "Proj3TotalProfitOutputStreamsTopic"));

        server.createContext("/stats/average-purchases-per-event",
                exchange -> handleStats(exchange, "Proj3AveragePurchasesPerEventOutputStreamsTopic"));

        server.createContext("/stats/average-purchases-all-events",
                exchange -> handleStats(exchange, "Proj3AveragePurchasesAllEventsOutputStreamsTopic"));

        server.createContext("/stats/highest-profit-event",
                exchange -> handleStats(exchange, "Proj3HighestProfitEventOutputStreamsTopic"));

        server.createContext("/stats/revenue-windowed",
                exchange -> handleStats(exchange, "Proj3TotalRevenueWindowedOutputStreamsTopic"));

        server.createContext("/stats/expenses-windowed",
                exchange -> handleStats(exchange, "Proj3TotalExpensesWindowedOutputStreamsTopic"));

        server.createContext("/stats/profit-windowed",
                exchange -> handleStats(exchange, "Proj3TotalProfitWindowedOutputStreamsTopic"));

        server.createContext("/stats/city-highest-sales-per-event",
                exchange -> handleStats(exchange, "Proj3CityHighestSalesPerEventOutputStreamsTopic"));

        server.setExecutor(null);
        server.start();

        System.out.println("Event Tickets REST API running at http://localhost:" + PORT);
        System.out.println("Database URL: " + DB_URL);
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    private static void handleHealth(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendError(exchange, 405, "Method not allowed");
            return;
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "UP");
        response.put("service", "Event Tickets REST API");

        sendJson(exchange, 200, response);
    }

    private static void handleCities(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();

            if (method.equalsIgnoreCase("GET")) {
                listCities(exchange);
            } else if (method.equalsIgnoreCase("POST")) {
                addCity(exchange);
            } else {
                sendError(exchange, 405, "Method not allowed");
            }

        } catch (Exception e) {
            sendError(exchange, 500, e.getMessage());
        }
    }

    private static void listCities(HttpExchange exchange) throws SQLException, IOException {
        String sql = "SELECT city_id, city_name FROM cities ORDER BY city_id";

        List<Map<String, Object>> cities = new ArrayList<>();

        try (
                Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()
        ) {
            while (rs.next()) {
                Map<String, Object> city = new LinkedHashMap<>();
                city.put("cityId", rs.getInt("city_id"));
                city.put("cityName", rs.getString("city_name"));
                cities.add(city);
            }
        }

        sendJson(exchange, 200, cities);
    }

    private static void addCity(HttpExchange exchange) throws SQLException, IOException {
        JsonObject body = readJsonBody(exchange);

        if (!body.has("cityName") || body.get("cityName").getAsString().isBlank()) {
            sendError(exchange, 400, "Missing required field: cityName");
            return;
        }

        String cityName = body.get("cityName").getAsString();

        String sql = """
                INSERT INTO cities (city_name)
                VALUES (?)
                RETURNING city_id, city_name
                """;

        try (
                Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            stmt.setString(1, cityName);

            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();

                Map<String, Object> city = new LinkedHashMap<>();
                city.put("cityId", rs.getInt("city_id"));
                city.put("cityName", rs.getString("city_name"));

                sendJson(exchange, 201, city);
            }
        }
    }

    private static void handleEvents(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();

            if (method.equalsIgnoreCase("GET")) {
                listEvents(exchange);
            } else if (method.equalsIgnoreCase("POST")) {
                addEvent(exchange);
            } else {
                sendError(exchange, 405, "Method not allowed");
            }

        } catch (Exception e) {
            sendError(exchange, 500, e.getMessage());
        }
    }

    private static void listEvents(HttpExchange exchange) throws SQLException, IOException {
        String sql = """
                SELECT event_id, event_name, city, category, base_ticket_price
                FROM events
                ORDER BY event_id
                """;

        List<Map<String, Object>> events = new ArrayList<>();

        try (
                Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()
        ) {
            while (rs.next()) {
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("eventId", rs.getString("event_id"));
                event.put("eventName", rs.getString("event_name"));
                event.put("city", rs.getString("city"));
                event.put("category", rs.getString("category"));
                event.put("baseTicketPrice", rs.getBigDecimal("base_ticket_price"));
                events.add(event);
            }
        }

        sendJson(exchange, 200, events);
    }

    private static void addEvent(HttpExchange exchange) throws SQLException, IOException {
        JsonObject body = readJsonBody(exchange);

        List<String> requiredFields = List.of(
                "eventId",
                "eventName",
                "city",
                "category",
                "baseTicketPrice"
        );

        for (String field : requiredFields) {
            if (!body.has(field) || body.get(field).getAsString().isBlank()) {
                sendError(exchange, 400, "Missing required field: " + field);
                return;
            }
        }

        String eventId = body.get("eventId").getAsString();
        String eventName = body.get("eventName").getAsString();
        String city = body.get("city").getAsString();
        String category = body.get("category").getAsString();
        BigDecimal baseTicketPrice = body.get("baseTicketPrice").getAsBigDecimal();

        String sql = """
                INSERT INTO events (event_id, event_name, city, category, base_ticket_price)
                VALUES (?, ?, ?, ?, ?)
                RETURNING event_id, event_name, city, category, base_ticket_price
                """;

        try (
                Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            stmt.setString(1, eventId);
            stmt.setString(2, eventName);
            stmt.setString(3, city);
            stmt.setString(4, category);
            stmt.setBigDecimal(5, baseTicketPrice);

            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();

                Map<String, Object> event = new LinkedHashMap<>();
                event.put("eventId", rs.getString("event_id"));
                event.put("eventName", rs.getString("event_name"));
                event.put("city", rs.getString("city"));
                event.put("category", rs.getString("category"));
                event.put("baseTicketPrice", rs.getBigDecimal("base_ticket_price"));

                sendJson(exchange, 201, event);
            }
        }
    }

    private static void handleStats(HttpExchange exchange, String tableName) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendError(exchange, 405, "Method not allowed");
            return;
        }

        try {
            List<Map<String, Object>> results = readResultsTable(tableName);
            sendJson(exchange, 200, results);
        } catch (SQLException e) {
            sendError(
                    exchange,
                    500,
                    "Could not read table " + tableName + ". " +
                            "Confirm that the sink connector has already created/populated this table. " +
                            "Details: " + e.getMessage()
            );
        }
    }

    private static List<Map<String, Object>> readResultsTable(String tableName) throws SQLException {
        String sql = "SELECT id, \"eventName\", value FROM \"" + tableName + "\" ORDER BY id";

        List<Map<String, Object>> results = new ArrayList<>();

        try (
                Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()
        ) {
            while (rs.next()) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("id", rs.getString("id"));
                result.put("eventName", rs.getString("eventName"));
                result.put("value", rs.getDouble("value"));
                results.add(result);
            }
        }

        return results;
    }

    private static JsonObject readJsonBody(HttpExchange exchange) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(
                exchange.getRequestBody(),
                StandardCharsets.UTF_8
        )) {
            return gson.fromJson(reader, JsonObject.class);
        }
    }

    private static void sendJson(HttpExchange exchange, int statusCode, Object data) throws IOException {
        String json = gson.toJson(data);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("error", message);

        sendJson(exchange, statusCode, error);
    }
}