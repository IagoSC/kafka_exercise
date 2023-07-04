import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
public class JsonNodeSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // @Override
    // public void configure(Map<String, ?> configs, boolean isKey) {
    //     // No configuration needed for this serializer
    // }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        try {
            if (data == null) {
                return null;
            } else {
                return objectMapper.writeValueAsBytes(data);
            }
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    // @Override
    // public void close() {
    //     // No resources to close for this serializer
    // }
}
