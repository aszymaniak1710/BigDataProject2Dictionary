package com.example.bigdata;

import com.example.bigdata.dictionary.MachineDictionary;
import com.example.bigdata.models.MachineEvent;
import com.example.bigdata.serde.JsonPOJOSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MachineStatsApp {

    private static final Logger logger = LoggerFactory.getLogger(MachineStatsApp.class);

    public static void main(String[] args) {
        //config
        Properties appConfig = loadProperties();

        //static dictionary
        MachineDictionary dictionary = loadDictionary(appConfig);

        //topology
        Properties props = createStreamsProperties(appConfig);
        Topology topology = buildTopology(appConfig, dictionary);

        //create and start kafka streams
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.setStateListener((newState, oldState) -> {
                logger.info("[STREAMS] State change: {} -> {}", oldState, newState);
            });
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    dictionary.stopFileWatcher();
                    latch.countDown();
                }
            });
            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = MachineStatsApp.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("[APP] application.properties not found in classpath. Exiting.");
                System.exit(1);
            }
            props.load(input);
        } catch (IOException e) {
            logger.error("[APP] Failed to load application.properties: {}", e.getMessage(), e);
            System.exit(1);
        }
        logConfiguration(props);
        return props;
    }

    private static void logConfiguration(Properties appConfig) {
        String bootstrapServers = appConfig.getProperty("kafka.bootstrap.servers");
        String outputTopic = appConfig.getProperty("kafka.topic.output");
        String dictionaryPath = appConfig.getProperty("dictionary.path");
        logger.info("[APP] Configuration loaded:");
        logger.info("  - Bootstrap servers: {}", bootstrapServers);
        logger.info("  - Input topic: {}", inputTopic);
        logger.info("  - Output topic: {}", outputTopic);
        logger.info("  - Dictionary: {}", dictionaryPath);
    }

    private static MachineDictionary loadDictionary(Properties appConfig) {
        String dictionaryPath = appConfig.getProperty("dictionary.path");

        MachineDictionary dictionary = MachineDictionary.getInstance();
        dictionary.loadFromFile(dictionaryPath);
        dictionary.startFileWatcher();

        return dictionary;
    }

    private static Properties createStreamsProperties(Properties appConfig) {
        String bootstrapServers = appConfig.getProperty("kafka.bootstrap.servers");
        String applicationId = appConfig.getProperty("kafka.application.id");
        String processingGuarantee = appConfig.getProperty("kafka.processing.guarantee");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        return props;
    }

    private static Topology buildTopology(Properties appConfig, MachineDictionary dictionary) {
        String outputTopic = appConfig.getProperty("kafka.topic.dictionary");
        StreamsBuilder builder = new StreamsBuilder();
        final Serde<MachineEvent> machineEventSerde = new JsonPOJOSerde<>(MachineEvent.class, false);
        KStream<String, MachineEvent> inputEvents = builder.stream(dictionary., Consumed.with(Serdes.String(), machineEventSerde));
        KStream<String, MachineEvent> processedEvents = inputEvents.peek((key, event) -> logger.info(event.toString()));
        processedEvents.to(outputTopic, Produced.with(Serdes.String(), machineEventSerde));
        return builder.build();
    }
}

