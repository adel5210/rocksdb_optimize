package com.adel.fastcache;

import com.adel.fastcache.rocksdb.TestRunRocksDbRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StopWatch;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class FastCacheApplication implements CommandLineRunner {

    private static final Logger log = LogManager.getLogger(FastCacheApplication.class);
    private static final String testFile = "C:\\Users\\Adel.Albediwy\\Desktop\\bigfile.txt";
    private final TestRunRocksDbRepository testRunRocksDbRepository;
    private final Map<String, String> data = new HashMap<>();

    public FastCacheApplication(TestRunRocksDbRepository testRunRocksDbRepository) {
        this.testRunRocksDbRepository = testRunRocksDbRepository;
    }

    public static void main(String[] args) {
        SpringApplication.run(FastCacheApplication.class, args);
    }

    @Override
    public void run(final String... args) throws Exception {
        log.info("Starting FastCacheApplication...");
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Files.readAllLines(Paths.get(testFile))
                .stream()
                .forEach(line -> {
//                    data.put(UUID.randomUUID().toString(), line);
                    testRunRocksDbRepository.save(UUID.randomUUID().toString(), line);
                });

        final AtomicInteger count = new AtomicInteger(0);
        testRunRocksDbRepository.findAll().forEach(run -> count.incrementAndGet());
//        data.values().forEach(run -> count.incrementAndGet());

        log.info("Total number of lines: {}", count.get());

        stopWatch.stop();
        log.info("process time: {} sec", stopWatch.getTotalTimeSeconds());
    }
}
