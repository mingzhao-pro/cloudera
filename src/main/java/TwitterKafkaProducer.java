import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.BasicConfigurator;

// NOTE: if it cannot resolve the name, add .local in default domain search

public class TwitterKafkaProducer {

    private static final String topic = "twitter";

    private static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-vm:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Initialize Twitter stream API
        //
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("macron"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret); // Twitter uses OAuth v1

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();

        //while (true) {
        for (int i = 0; i < 100000; ++i) {
            final String tweet = queue.take();
//            System.out.println(tweet);

            producer.send(new ProducerRecord<>(topic, tweet));
        }

        producer.close();
        client.stop();
    }

    public static void main(String[] args) {
        try {
            // Initialize Log4j
            //
//            BasicConfigurator.configure();

            // Twitter API keys
            //
            final String consumer_key = "gVvexH2JOMCGU5mmWjeTkpWDv";
            final String consumer_secret = "aKs0SHxxP9AZzdmRcS8SZsMSqqkYe3iSmSeFU5TBqTRU7uWZMp";
            final String access_token_key = "4185703943-2XnSLq8Qf0uvYPJUpNlQBtySEoqTxarT7wAtNVS";
            final String access_token_secret = "kUGAHX8fCFMasoKSSTP5rov5woOWqN9ntKuLx9HwIce8L";

            // Launch the Twitter/Kafka producer
            //
            TwitterKafkaProducer.run(consumer_key, consumer_secret, access_token_key, access_token_secret);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
