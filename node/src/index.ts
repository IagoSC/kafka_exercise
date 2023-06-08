import { KafkaStreams } from "kafka-streams";

const kafkaStreams = new KafkaStreams({kafkaHost: "172.21.86.231:9092"});

const streamHandler = (topic: string) => {
    console.log("start")
    const kstream = kafkaStreams.getKStream(topic);

    console.log(kstream)
    kstream.forEach(el => console.log(el))

    kstream.map(event => JSON.parse(event))
        .filter(({time}) => time === "morning")
        .map(event => ({message: `${event.intensity === "good" ? "bela" : "horrorosa"} manhã do dia ${event.date}`}))
        .to("belas_manhas")

}

streamHandler("barrote")