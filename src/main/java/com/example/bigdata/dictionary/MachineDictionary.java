package com.example.bigdata.dictionary;

import com.example.bigdata.models.Machine;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private WatchService watchService;
    private Thread watcherThread;
    private volatile boolean running = false;

    private MachineDictionary() {
    }

    public synchronized static MachineDictionary getInstance() {
        if (instance == null) {
            instance = new MachineDictionary();
        }
        return instance;
    }

    public void loadFromFile(String path) {
        this.dictionaryPath = path;
        reloadDictionary();
    }

    private synchronized void reloadDictionary() {
        try {
            File file = new File(dictionaryPath);
            if (!file.exists()) {
                logger.error("[DICTIONARY] File not found: {}", dictionaryPath);
                return;
            }
            Machine[] machineArray = objectMapper.readValue(file, Machine[].class);
            machines.clear();
            for (Machine machine : machineArray) {
                machines.put(machine.machineId, machine);
            }
            logger.info("[DICTIONARY RELOAD] Loaded {} machines from {}", machines.size(), dictionaryPath);
            logMachinesList();
        } catch (IOException e) {
            logger.error("[DICTIONARY ERROR] Failed to load dictionary from {}: {}", dictionaryPath, e.getMessage(), e);
        }
    }

    private void logMachinesList() {
        if (machines.isEmpty()) {
            logger.info("[DICTIONARY] No machines loaded");
            return;
        }
        logger.info("[DICTIONARY] Loaded machines:");
        machines.values().stream()
                .sorted(Comparator.comparing(m -> m.machineId))
                .forEach(m -> logger.info(m.toString()));
    }

    public int size() {
        return machines.size();
    }

    public void startFileWatcher() {
        if (running) {
            logger.warn("[DICTIONARY] File watcher already running");
            return;
        }
        try {
            watchService = FileSystems.getDefault().newWatchService();
            Path dictionaryDir = Paths.get(dictionaryPath).getParent();
            String fileName = Paths.get(dictionaryPath).getFileName().toString();
            dictionaryDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);
            running = true;
            watcherThread = new Thread(() -> {
                logger.info("[DICTIONARY WATCHER] Started monitoring {}", dictionaryPath);
                runWatchLoop(fileName);
                logger.info("[DICTIONARY WATCHER] Stopped");
            }, "MachineDictionary-Watcher");

            watcherThread.setDaemon(true);
            watcherThread.start();

        } catch (IOException e) {
            logger.error("[DICTIONARY WATCHER] Failed to start: {}", e.getMessage(), e);
        }
    }


    public void stopFileWatcher() {
        if (!running) {
            return;
        }
        running = false;
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                logger.error("[DICTIONARY WATCHER] Error closing watch service: {}", e.getMessage());
            }
        }
        if (watcherThread != null) {
            try {
                watcherThread.join(5000);  // Max 5s czekania
            } catch (InterruptedException e) {
                logger.warn("[DICTIONARY WATCHER] Failed to join watcher thread");
            }
        }
        logger.info("[DICTIONARY] File watcher stopped");
    }

    private void runWatchLoop(String fileName) {
        while (running) {
            try {
                WatchKey key = watchService.take();
                processWatchKey(key, fileName);
                key.reset();
            } catch (ClosedWatchServiceException ignored) {
            } catch (Exception e) {
                logger.error("[DICTIONARY WATCHER] Error: {}", e.getMessage(), e);
            }
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
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        reloadDictionary();
    }
}
