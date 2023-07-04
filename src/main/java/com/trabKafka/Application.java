package com.trabKafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import com.trabKafka.JsonNodeSerializer;
import com.trabKafka.JsonNodeDeserializer;

import java.util.Properties;

public class Application {

    // public void example(){
    //     Serde<String> stringSerde = Serdes.String();
    //     final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    //     final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
    //     final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    //     // Configuring consumer
    //     final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);
    // }


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "trab-kafka");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(JsonNodeSerializer.class, JsonNodeDeserializer.class);

        KStream<String, JsonNode> inputTopic = builder.stream("input-topic",
                Consumed.with(Serdes.String(), jsonSerde));

        inputTopic.peek((key, value) -> {
            // Process the JSON message
            System.out.println("Received JSON: " + value);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}