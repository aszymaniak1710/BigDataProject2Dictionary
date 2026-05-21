package com.example.bigdata.dictionary;

import com.example.bigdata.models.Machine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MachineDictionary {

    private final ConcurrentHashMap<String, Machine> machines = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(MachineDictionary.class);
    private static MachineDictionary instance;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String dictionaryPath;
    private String dictionaryTopic;
    private KafkaProducer<String, String> kafkaProducer;
    private WatchService watchService;

    private MachineDictionary() {
    }

    public synchronized static MachineDictionary getInstance() {
        if (instance == null) {
            instance = new MachineDictionary();
        }
        return instance;
    }

    public void init(String path, String bootstrapServers, String dictionaryTopic) {
        this.dictionaryPath = path;
        this.dictionaryTopic = dictionaryTopic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.kafkaProducer = new KafkaProducer<>(props);
        reloadAndPublishDictionary();
    }

    private synchronized void reloadAndPublishDictionary() {
        try {
            File file = new File(dictionaryPath);
            if (!file.exists()) {
                logger.error("[DICTIONARY] File not found: {}", dictionaryPath);
                return;
            }
            Machine[] machineArray = objectMapper.readValue(file, Machine[].class);

            // 1. Tworzymy pomocniczy zbiór kluczy, które AKTUALNIE są w pliku JSON
            Set<String> currentJsonKeys = new HashSet<>();

            for (Machine newMachine : machineArray) {
                currentJsonKeys.add(newMachine.machineId);
                Machine oldMachine = machines.get(newMachine.machineId);

                if (oldMachine == null || !oldMachine.equals(newMachine)) {
                    String machineJson = objectMapper.writeValueAsString(newMachine);
                    ProducerRecord<String, String> record = new ProducerRecord<>(dictionaryTopic, newMachine.machineId, machineJson);
                    kafkaProducer.send(record);
                    machines.put(newMachine.machineId, newMachine);
                    logger.info("[DICTIONARY] Updated/Added machine: {}", newMachine.machineId);
                }
            }

            machines.keySet().stream()
                    .filter(rememberedKey -> !currentJsonKeys.contains(rememberedKey))
                    .toList()
                    .forEach(deletedKey -> {
                        ProducerRecord<String, String> tombstoneRecord = new ProducerRecord<>(dictionaryTopic, deletedKey, null);
                        kafkaProducer.send(tombstoneRecord);
                        machines.remove(deletedKey);
                        logger.warn("[DICTIONARY DETECTED DELETION] Sent TOMBSTONE for machine: {}", deletedKey);
                    });
        } catch (IOException e) {
            logger.error("[DICTIONARY ERROR] Failed to process/publish dictionary: {}", e.getMessage(), e);
        }
    }

    public void startMonitoring() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            Path dictionaryDir = Paths.get(dictionaryPath).getParent();
            String fileName = Paths.get(dictionaryPath).getFileName().toString();
            dictionaryDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);
            runWatchLoop(fileName);
        } catch (IOException e) {
            logger.error("[DICTIONARY WATCHER] Failed to start: {}", e.getMessage(), e);
        }
    }

    private void runWatchLoop(String fileName) {
        try {
            while (true) {
                WatchKey key = watchService.take();
                processWatchKey(key, fileName);
                key.reset();
            }
        } catch (ClosedWatchServiceException ignored) {
        } catch (Exception e) {
            logger.error("[DICTIONARY WATCHER] Error: {}", e.getMessage(), e);
        }
    }

    private void processWatchKey(WatchKey key, String fileName) {
        for (WatchEvent<?> event : key.pollEvents()) {
            Path changedPath = (Path) event.context();
            if (changedPath.toString().equals(fileName)) {
                handleFileChange();
            }
        }
    }

    private void handleFileChange() {
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        reloadAndPublishDictionary();
    }

    public void close() {
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                logger.error("[DICTIONARY WATCHER] Error closing watch service: {}", e.getMessage());
            }
        }
        kafkaProducer.close();
        logger.info("[DICTIONARY] Kafka Producer closed");
    }
}