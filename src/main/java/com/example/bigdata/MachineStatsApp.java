package com.example.bigdata;

import com.example.bigdata.dictionary.MachineDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MachineStatsApp {

    private static final Logger logger = LoggerFactory.getLogger(MachineStatsApp.class);

    public static void main(String[] args) {
        Properties appConfig = loadProperties();

        MachineDictionary dictionary = MachineDictionary.getInstance();
        String dictionaryPath = appConfig.getProperty("dictionary.path");
        String bootstrapServers = appConfig.getProperty("kafka.bootstrap.servers");
        String dictionaryTopic = appConfig.getProperty("kafka.topic.dictionary");

        dictionary.init(dictionaryPath, bootstrapServers, dictionaryTopic);

        Runtime.getRuntime().addShutdownHook(new Thread("publisher-shutdown-hook") {
            @Override
            public void run() {
                logger.info("[APP] Shutting down application...");
                dictionary.close();
            }
        });
        dictionary.startMonitoring();
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
        return props;
    }
}