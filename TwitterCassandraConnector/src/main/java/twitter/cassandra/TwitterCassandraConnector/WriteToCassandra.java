package twitter.cassandra.TwitterCassandraConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Date;
import java.util.Arrays;
import java.util.Properties;

public class WriteToCassandra
{
    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.99.101:9102");
        props.put("group.id", "grp-1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);

        final CassandraConnector client = new CassandraConnector();
        client.connect("192.168.99.101", 9042);

        consumer.subscribe(Arrays.asList("values"));
        while (true)
        {
            ConsumerRecords<String, Long> records = consumer.poll(1000);
            for (ConsumerRecord<String, Long> record : records)
            {
                String[] key = record.key().split("::");
                String time = key[0];
                String category = key[1];
                client.getSession().execute("INSERT INTO edureka.productdetails (time,category, count) " +
                                "VALUES (?, ?, ?)", new Date(Long.parseLong(time)),category,record.value());
            }
        }

    }
}
