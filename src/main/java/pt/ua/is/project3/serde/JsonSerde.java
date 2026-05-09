package pt.ua.is.project3.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerde<T> implements Serde<T> {

    private final Gson gson = new Gson();
    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // No configuration needed
            }

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null;
                }

                return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
                // Nothing to close
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // No configuration needed
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null || data.length == 0) {
                    return null;
                }

                String json = new String(data, StandardCharsets.UTF_8);
                return gson.fromJson(json, targetType);
            }

            @Override
            public void close() {
                // Nothing to close
            }
        };
    }
}