import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class CityLogCount {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "city-count-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, JsonNode> cityLogs =
                builder.stream(Serdes.String(), jsonSerde, "city-logs-input");


        // create the initial json object for balances
        ObjectNode initialCount = JsonNodeFactory.instance.objectNode();
        initialCount.put("count", 0);
        KTable<String, JsonNode> cityCount = cityLogs
                .groupByKey(Serdes.String(), jsonSerde)
                .aggregate(
                        () -> initialCount,
                        (key, fromProducer, temp) -> updateCount(fromProducer, temp),
                        jsonSerde,
                        "temp"
                );

        cityCount.to(Serdes.String(), jsonSerde,"city-logs-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode updateCount(JsonNode fromProducer, JsonNode temp) {
        // create a new balance json object
        ObjectNode newCount = JsonNodeFactory.instance.objectNode();
        newCount.put("count", temp.get("count").asInt() + 1);
        newCount.put("time",fromProducer.get("time"));
        newCount.put("city",fromProducer.get("city"));
        newCount.put("logLevel",fromProducer.get("logLevel"));
        newCount.put("logMessage",fromProducer.get("logMessage"));

        //System.out.println(newCount.toString());
        return newCount;
    }
}
