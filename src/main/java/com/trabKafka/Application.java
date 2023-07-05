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
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.86.231:9092");

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonNodeSerializer(), new JsonNodeDeserializer());

        KStream<String, JsonNode> inputTopic = builder.stream("weather-monitor",
                Consumed.with(Serdes.String(), jsonSerde));

        inputTopic.peek((key, value) -> {
            // Process the JSON message
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");
            System.out.println("**********************");

            System.out.println("**********************");
            System.out.println("Received JSON: " + value);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}