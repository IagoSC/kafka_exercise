package com.trabKafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // @Override
    // public void configure(Map<String, ?> configs, boolean isKey) {}

   @Override
    public JsonNode deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
                return objectMapper.readTree(data);
            }
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }
    
    // @Override
    // public void close() {}
}
