package exercicio;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.ser.std.JsonValueSerializer;

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;

class Event {
	public String date;
	public String time; // "morning" | "midday" | "evening" | "night"
	public String metricType; // "temperature_2m" | "precipitation_probability" | "windspeed_10m" | "uv_index"
	public String intensity; // "good" | "bad"

	public Event(){
		this(null);
	}

	public Event(String date, String time, String metricType, String intensity){
		this.date = date;
		this.time = time;
		this.metricType = metricType;
		this.intensity = intensity;
	}
}


public class EventDeserializer extends StdDeserializer<Event> { 

    public EventDeserializer() { 
        this(null); 
    } 

    public EventDeserializer(Class<?> vc) { 
        super(vc); 
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctxt) 
      throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        String topic = (String) ((StringNode) node.get("topic")).textValue();
        JsonNode messages = node.get("messages").foundValues();
		String date = messages[0].get("value").get("date").textValue();
		String time = messages[0].get("value").get("time").textValue();
		String metricType = messages[0].get("value").get("metricType").textValue();
		String intensity = messages[0].get("value").get("intensity").textValue();

        return new Event(date);
    }
}

class MainClass{  
	public static void main(String[] args){    
		String local = args[2];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercicio_streams");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); 
		// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");

		Serde<String> stringSerde = Serdes.String();
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> eventStream = builder.stream(local, Consumed.with(stringSerde, stringSerde))
			.peek((key, message) -> System.out.println("Evento meteorológico: " + message));



		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		
		System.out.println("Stream criado");
	}
}  