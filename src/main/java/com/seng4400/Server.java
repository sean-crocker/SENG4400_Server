package com.seng4400;

import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.appengine.repackaged.com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

/**
 * The Server driver class is responsible for publishing to the publish/subscribe queue. The server randomly generates a
 * value between 1 and a given maximum and publishes the message to the queue.
 *
 * @author  Sean Crocker
 * @version 1.0
 * @since   01/06/2022
 */
public class Server {

    /**
     * Function responsible for creating and setting up the producer by applying configurations. Once the configurations
     * are set the producer is returned.
     *
     * @return          the producer with properties set.
     */
    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ass2");
        return new KafkaProducer<>(props);
    }

    /**
     * Driver function which can take one or two optional program arguments to set the delay and maximum range with.
     * If there are more than two arguments given or one argument is not an integer, the program throws an illegal
     * argument exception or number format exception. Once the arguments have been retrieved the main function can run.
     *
     * @param args                      An array of command line arguments that can be used to define the delay time and
     *                                  max range of random number generator
     * @throws NumberFormatException    If any program argument is not an integer
     */
    public static void main(String[] args) throws NumberFormatException {
        if (args.length > 2)
            throw new IllegalArgumentException("Error. Program must run with a maximum of two arguments.");
        int delay = args.length > 0 ? Integer.parseInt(args[0]) : 1000;         // Default delay of 1000ms
        int maximum = args.length == 2 ? Integer.parseInt(args[1]) : 1000000;   // Default maximum is 1000000
        run(delay, maximum);
    }

    /**
     * Main function responsible for publishing to the queue repeatedly with a delay either given or default value.
     * Before each publish, a random number is generated and assigned to the value of the question for the message sent.
     * If publishing is successful, the question with its assigned value is printed to the console.
     *
     * @param delay     the time in milliseconds between publishing
     * @param maximum   the maximum range for the random number generator
     */
    private static void run(int delay, int maximum) {
        Producer<String, String> producer = createProducer();               // Create the producer
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                int primeMax = (int) (Math.random() * (maximum - 2) + 1);   // Generate a random number
                producer.send(new ProducerRecord<>("seng4400", null, System.currentTimeMillis(),
                        "question", String.valueOf(primeMax)), (recordMetadata, e) -> {
                    if (recordMetadata != null) {
                        Map<String, Object> data = new HashMap<>();
                        Gson gson = new GsonBuilder().setPrettyPrinting().create();
                        data.put("question", String.valueOf(primeMax));
                        System.out.println(gson.toJson(data));
                    }
                    else
                        e.printStackTrace();
                });
            }
        }, 0, delay);
    }
}
