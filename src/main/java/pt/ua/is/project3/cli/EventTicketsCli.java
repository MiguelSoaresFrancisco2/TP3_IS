package pt.ua.is.project3.cli;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class EventTicketsCli {

    private static final String API_BASE_URL = "http://localhost:8080";

    private static final Gson gson = new Gson();
    private static Scanner scanner;

    public static void main(String[] args) {
        scanner = new Scanner(System.in);

        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║      SISTEMA DE EVENTOS E VENDA DE TICKETS    ║");
        System.out.println("║             Administrator Console             ║");
        System.out.println("╚════════════════════════════════════════════════╝");

        checkApiHealth();

        boolean running = true;

        while (running) {
            displayMainMenu();
            int choice = getIntInput("Escolha uma opção: ");

            switch (choice) {
                case 1:
                    addCity();
                    break;
                case 2:
                    listCities();
                    break;
                case 3:
                    addEvent();
                    break;
                case 4:
                    listEvents();
                    break;
                case 5:
                    statisticsMenu();
                    break;
                case 6:
                    System.out.println("\n✓ A sair da aplicação...");
                    running = false;
                    break;
                default:
                    System.out.println("\n✗ Opção inválida.");
                    break;
            }

            System.out.println();
        }

        scanner.close();
    }

    private static void displayMainMenu() {
        System.out.println();
        System.out.println("┌─ MENU PRINCIPAL ─────────────────────────────┐");
        System.out.println("│ 1. Adicionar Cidade                          │");
        System.out.println("│ 2. Listar Cidades                            │");
        System.out.println("│ 3. Adicionar Evento                          │");
        System.out.println("│ 4. Listar Eventos                            │");
        System.out.println("│ 5. Ver Estatísticas                          │");
        System.out.println("│ 6. Sair                                      │");
        System.out.println("└──────────────────────────────────────────────┘");
    }

    private static void checkApiHealth() {
        try {
            String response = makeGetRequest(API_BASE_URL + "/health");

            if (response != null) {
                System.out.println("\n✓ REST API disponível em " + API_BASE_URL);
            } else {
                System.out.println("\n⚠ Não foi possível confirmar o estado da REST API.");
            }
        } catch (Exception e) {
            System.out.println("\n⚠ REST API não está acessível em " + API_BASE_URL);
            System.out.println("  Confirma se a classe EventTicketsRestApi está a correr.");
        }
    }

    private static void addCity() {
        System.out.println("\n─── ADICIONAR CIDADE ───");

        String cityName = getStringInput("Nome da cidade: ");

        if (cityName.isBlank()) {
            System.out.println("✗ O nome da cidade não pode estar vazio.");
            return;
        }

        JsonObject body = new JsonObject();
        body.addProperty("cityName", cityName);

        try {
            String response = makePostRequest(API_BASE_URL + "/cities", body.toString());

            if (response != null) {
                JsonObject city = gson.fromJson(response, JsonObject.class);

                System.out.println("✓ Cidade adicionada com sucesso.");
                System.out.println("  ID: " + city.get("cityId").getAsInt());
                System.out.println("  Nome: " + city.get("cityName").getAsString());
            }
        } catch (Exception e) {
            System.out.println("✗ Erro ao adicionar cidade: " + e.getMessage());
        }
    }

    private static void listCities() {
        System.out.println("\n─── LISTAR CIDADES ───");

        try {
            String response = makeGetRequest(API_BASE_URL + "/cities");

            if (response == null) {
                return;
            }

            JsonArray cities = gson.fromJson(response, JsonArray.class);

            if (cities.size() == 0) {
                System.out.println("Ainda não existem cidades registadas.");
                return;
            }

            System.out.println("┌────────────┬────────────────────────────┐");
            System.out.println("│ ID         │ Cidade                     │");
            System.out.println("├────────────┼────────────────────────────┤");

            for (int i = 0; i < cities.size(); i++) {
                JsonObject city = cities.get(i).getAsJsonObject();

                int cityId = city.get("cityId").getAsInt();
                String cityName = city.get("cityName").getAsString();

                System.out.printf("│ %-10d │ %-26s │%n", cityId, truncate(cityName, 26));
            }

            System.out.println("└────────────┴────────────────────────────┘");

        } catch (Exception e) {
            System.out.println("✗ Erro ao listar cidades: " + e.getMessage());
        }
    }

    private static void addEvent() {
        System.out.println("\n─── ADICIONAR EVENTO ───");

        String eventId = getStringInput("ID do evento (ex: E6): ");
        String eventName = getStringInput("Nome do evento: ");
        String city = getStringInput("Cidade: ");
        String category = getStringInput("Categoria (Technology/Music/Sports/Entertainment): ");
        String priceInput = getStringInput("Preço base do bilhete: ");

        if (
                eventId.isBlank()
                        || eventName.isBlank()
                        || city.isBlank()
                        || category.isBlank()
                        || priceInput.isBlank()
        ) {
            System.out.println("✗ Todos os campos são obrigatórios.");
            return;
        }

        double baseTicketPrice;

        try {
            baseTicketPrice = Double.parseDouble(priceInput);
        } catch (NumberFormatException e) {
            System.out.println("✗ O preço base deve ser um número válido.");
            return;
        }

        JsonObject body = new JsonObject();
        body.addProperty("eventId", eventId);
        body.addProperty("eventName", eventName);
        body.addProperty("city", city);
        body.addProperty("category", category);
        body.addProperty("baseTicketPrice", baseTicketPrice);

        try {
            String response = makePostRequest(API_BASE_URL + "/events", body.toString());

            if (response != null) {
                JsonObject event = gson.fromJson(response, JsonObject.class);

                System.out.println("✓ Evento adicionado com sucesso.");
                System.out.println("  ID: " + event.get("eventId").getAsString());
                System.out.println("  Nome: " + event.get("eventName").getAsString());
                System.out.println("  Cidade: " + event.get("city").getAsString());
                System.out.println("  Categoria: " + event.get("category").getAsString());
                System.out.println("  Preço base: € " + event.get("baseTicketPrice").getAsDouble());
            }

        } catch (Exception e) {
            System.out.println("✗ Erro ao adicionar evento: " + e.getMessage());
        }
    }

    private static void listEvents() {
        System.out.println("\n─── LISTAR EVENTOS ───");

        try {
            String response = makeGetRequest(API_BASE_URL + "/events");

            if (response == null) {
                return;
            }

            JsonArray events = gson.fromJson(response, JsonArray.class);

            if (events.size() == 0) {
                System.out.println("Ainda não existem eventos registados.");
                return;
            }

            System.out.println("┌────────┬──────────────────────┬──────────────┬────────────────┬────────────┐");
            System.out.println("│ ID     │ Evento               │ Cidade       │ Categoria      │ Preço Base │");
            System.out.println("├────────┼──────────────────────┼──────────────┼────────────────┼────────────┤");

            for (int i = 0; i < events.size(); i++) {
                JsonObject event = events.get(i).getAsJsonObject();

                String eventId = event.get("eventId").getAsString();
                String eventName = event.get("eventName").getAsString();
                String city = event.get("city").getAsString();
                String category = event.get("category").getAsString();
                double baseTicketPrice = event.get("baseTicketPrice").getAsDouble();

                System.out.printf(
                        "│ %-6s │ %-20s │ %-12s │ %-14s │ € %8.2f │%n",
                        truncate(eventId, 6),
                        truncate(eventName, 20),
                        truncate(city, 12),
                        truncate(category, 14),
                        baseTicketPrice
                );
            }

            System.out.println("└────────┴──────────────────────┴──────────────┴────────────────┴────────────┘");

        } catch (Exception e) {
            System.out.println("✗ Erro ao listar eventos: " + e.getMessage());
        }
    }

    private static void statisticsMenu() {
        boolean back = false;

        while (!back) {
            System.out.println("\n─── ESTATÍSTICAS ───");
            System.out.println("┌─ MENU DE ESTATÍSTICAS ───────────────────────┐");
            System.out.println("│ 1. Receita por Evento                        │");
            System.out.println("│ 2. Despesa por Evento                        │");
            System.out.println("│ 3. Lucro por Evento                          │");
            System.out.println("│ 4. Receita Total                             │");
            System.out.println("│ 5. Despesa Total                             │");
            System.out.println("│ 6. Lucro Total                               │");
            System.out.println("│ 7. Média de Venda por Evento                 │");
            System.out.println("│ 8. Média de Venda Global                     │");
            System.out.println("│ 9. Evento com Maior Lucro                    │");
            System.out.println("│ 10. Receita no Último Minuto                 │");
            System.out.println("│ 11. Despesa no Último Minuto                 │");
            System.out.println("│ 12. Lucro no Último Minuto                   │");
            System.out.println("│ 13. Cidade com Maior Venda por Evento        │");
            System.out.println("│ 14. Voltar                                   │");
            System.out.println("└──────────────────────────────────────────────┘");

            int choice = getIntInput("Escolha uma estatística: ");

            switch (choice) {
                case 1:
                    displayStatistic("/stats/revenue-per-event", "RECEITA POR EVENTO");
                    break;
                case 2:
                    displayStatistic("/stats/expenses-per-event", "DESPESA POR EVENTO");
                    break;
                case 3:
                    displayStatistic("/stats/profit-per-event", "LUCRO POR EVENTO");
                    break;
                case 4:
                    displayStatistic("/stats/total-revenue", "RECEITA TOTAL");
                    break;
                case 5:
                    displayStatistic("/stats/total-expenses", "DESPESA TOTAL");
                    break;
                case 6:
                    displayStatistic("/stats/total-profit", "LUCRO TOTAL");
                    break;
                case 7:
                    displayStatistic("/stats/average-purchases-per-event", "MÉDIA DE VENDA POR EVENTO");
                    break;
                case 8:
                    displayStatistic("/stats/average-purchases-all-events", "MÉDIA DE VENDA GLOBAL");
                    break;
                case 9:
                    displayStatistic("/stats/highest-profit-event", "EVENTO COM MAIOR LUCRO");
                    break;
                case 10:
                    displayStatistic("/stats/revenue-windowed", "RECEITA NO ÚLTIMO MINUTO");
                    break;
                case 11:
                    displayStatistic("/stats/expenses-windowed", "DESPESA NO ÚLTIMO MINUTO");
                    break;
                case 12:
                    displayStatistic("/stats/profit-windowed", "LUCRO NO ÚLTIMO MINUTO");
                    break;
                case 13:
                    displayStatistic("/stats/city-highest-sales-per-event", "CIDADE COM MAIOR VENDA POR EVENTO");
                    break;
                case 14:
                    back = true;
                    break;
                default:
                    System.out.println("✗ Opção inválida.");
                    break;
            }
        }
    }

    private static void displayStatistic(String endpoint, String title) {
        System.out.println("\n─── " + title + " ───");

        try {
            String response = makeGetRequest(API_BASE_URL + endpoint);

            if (response == null) {
                return;
            }

            JsonArray results = gson.fromJson(response, JsonArray.class);

            if (results.size() == 0) {
                System.out.println("Ainda não existem dados para esta estatística.");
                System.out.println("Confirma se os producers, Kafka Streams e Sink Connector já estão a correr.");
                return;
            }

            System.out.println("┌──────────────────────────────┬────────────────────────────────┬──────────────┐");
            System.out.println("│ ID                           │ Nome / Descrição               │ Valor        │");
            System.out.println("├──────────────────────────────┼────────────────────────────────┼──────────────┤");

            for (int i = 0; i < results.size(); i++) {
                JsonObject record = results.get(i).getAsJsonObject();

                String id = getStringOrDefault(record, "id", "-");
                String eventName = getStringOrDefault(record, "eventName", "-");
                double value = getDoubleOrDefault(record, "value", 0.0);

                System.out.printf(
                        "│ %-28s │ %-30s │ € %10.2f │%n",
                        truncate(id, 28),
                        truncate(eventName, 30),
                        value
                );
            }

            System.out.println("└──────────────────────────────┴────────────────────────────────┴──────────────┘");

        } catch (Exception e) {
            System.out.println("✗ Erro ao consultar estatística: " + e.getMessage());
        }
    }

    private static String makeGetRequest(String urlString) throws Exception {
        HttpURLConnection conn = createConnection(urlString, "GET");

        int responseCode = conn.getResponseCode();

        if (responseCode >= 200 && responseCode < 300) {
            return readResponse(conn);
        }

        printHttpError(conn, responseCode);
        return null;
    }

    private static String makePostRequest(String urlString, String jsonPayload) throws Exception {
        HttpURLConnection conn = createConnection(urlString, "POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();

        if (responseCode >= 200 && responseCode < 300) {
            return readResponse(conn);
        }

        printHttpError(conn, responseCode);
        return null;
    }

    private static HttpURLConnection createConnection(String urlString, String method) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(method);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.setRequestProperty("Accept", "application/json");

        return conn;
    }

    private static String readResponse(HttpURLConnection conn) throws Exception {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8)
        )) {
            StringBuilder response = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            return response.toString();
        }
    }

    private static void printHttpError(HttpURLConnection conn, int responseCode) {
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8)
            );

            StringBuilder errorBody = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                errorBody.append(line);
            }

            System.out.println("✗ Erro HTTP " + responseCode + ": " + errorBody);

        } catch (Exception e) {
            System.out.println("✗ Erro HTTP " + responseCode);
        }
    }

    private static int getIntInput(String prompt) {
        while (true) {
            try {
                System.out.print(prompt);
                return Integer.parseInt(scanner.nextLine().trim());
            } catch (NumberFormatException e) {
                System.out.println("✗ Introduz um número válido.");
            }
        }
    }

    private static String getStringInput(String prompt) {
        System.out.print(prompt);
        return scanner.nextLine().trim();
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "-";
        }

        if (value.length() <= maxLength) {
            return value;
        }

        if (maxLength <= 3) {
            return value.substring(0, maxLength);
        }

        return value.substring(0, maxLength - 3) + "...";
    }

    private static String getStringOrDefault(JsonObject object, String field, String defaultValue) {
        if (object == null || !object.has(field) || object.get(field).isJsonNull()) {
            return defaultValue;
        }

        return object.get(field).getAsString();
    }

    private static double getDoubleOrDefault(JsonObject object, String field, double defaultValue) {
        if (object == null || !object.has(field) || object.get(field).isJsonNull()) {
            return defaultValue;
        }

        try {
            return object.get(field).getAsDouble();
        } catch (Exception e) {
            return defaultValue;
        }
    }
}