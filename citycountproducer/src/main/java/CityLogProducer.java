import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class CityLogProducer {
    public static void main(String[] args) throws IOException, InterruptedException {

        String logPath = args[0];
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        FileReader fr = new FileReader(logPath+"/current_log.log");
        BufferedReader br = new BufferedReader(fr);

        while (true) {
            String line = br.readLine();
            if (line == null)
            {
                Thread.sleep(1*100);
            }
            else
            {

                try {
                    String logLine[]= line.split("\\t");
                    String time_=logLine[0];
                    String logLevel=logLine[1];
                    String city=logLine[2];
                    String logMessage = logLine[3];
                    ObjectNode logData= JsonNodeFactory.instance.objectNode();
                    logData.put("time",time_);
                    logData.put("city",city);
                    logData.put("logLevel",logLevel);
                    logData.put("logMessage",logMessage);

                    producer.send(new ProducerRecord<>("city-logs-input", city+time_, logData.toString()));


                    Thread.sleep(100);

                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        producer.close();
    }

}
