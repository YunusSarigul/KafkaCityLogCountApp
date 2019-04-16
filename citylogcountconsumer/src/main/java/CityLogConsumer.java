import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class CityLogConsumer {


    public static void main(String[] args) throws IOException, SQLException {
        String dbUser = args[0];
        String dbPassword = args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1"); // Default group
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("city-logs-output"));
        Connection con =(Connection) DriverManager.getConnection
                ("jdbc:mysql://localhost:3306/kafkabackup",dbUser,dbPassword);
        Statement stm ;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                // System.out.println("offset = " + record.offset() + ", value = " + record.value());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode nodeToInsert = mapper.readTree(record.value());
                String query = "insert into kafka_backup values(";
                query +=nodeToInsert.get("time")+","+nodeToInsert.get("logLevel")+","+nodeToInsert.get("city")+","+nodeToInsert.get("logMessage")+","+nodeToInsert.get("count")+");";
                stm = (Statement) con.createStatement();
                stm.executeUpdate(query);
                // System.out.println(query);
            }
        }

    }
}
