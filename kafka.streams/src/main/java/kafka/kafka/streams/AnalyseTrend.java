package kafka.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Created by Aride Chettali on 21-Dec-17.
 */
public class AnalyseTrend
{
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example1");
        //props.put(StreamsConfig.STATE_DIR_CONFIG, "steams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.101:9101");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1000");

        KStreamBuilder  builder = new KStreamBuilder ();
       KStream<String, String> source = builder.stream("tweets");
//        KStream<String, String> textLine = builder.stream("counter-in", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<Windowed<String>, Long> aggregatedTable = source
                       .selectKey((userId, tweet) -> {
                                  if(tweet.contains("android"))
                                      return "android";
                                  else if(tweet.contains("ios"))
                                  {
                                      return "ios";
                                  }
                                  else if(tweet.contains("blackberry"))
                                  {
                                      return "blackberry";
                                  }
                                  else
                                  {
                                      return "windows";
                                  }
                       })
                       .groupByKey()
                       .count(TimeWindows.of(60000));

        KStream<String, Long> aggregatedStream = aggregatedTable.toStream((window, val) -> window.window().start() + "::" + window.key());

        aggregatedStream.to(new Serdes.StringSerde(), new Serdes.LongSerde(), "values");

        KafkaStreams streams = new KafkaStreams(builder,props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}