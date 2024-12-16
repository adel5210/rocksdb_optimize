package com.adel.fastcache.rocksdb;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Adel.Albediwy
 */
public interface RockdbBaseRepository<V> extends AutoCloseable {

    int MAX_CORES = 4;

    RocksDB getDB();

    Class<V> getValueClass();

    default RocksDB initializedDB(final String dbName,
                                  final String filePath,
                                  final boolean canWrite,
                                  final boolean resetDBOnStartup,
                                  final boolean isPermanent) throws IOException {
        RocksDB.loadLibrary();

        final int maxCores = Runtime.getRuntime().availableProcessors();

        final Options options = new Options();
        options.prepareForBulkLoad();
        options.setCreateIfMissing(true);
        options.setOptimizeFiltersForHits(true);
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setAllow2pc(true);
        options.setAllowConcurrentMemtableWrite(true);
        options.setCompactionStyle(CompactionStyle.FIFO);
        options.setMaxOpenFiles(-1);
        options.setMaxBackgroundCompactions(4);
        options.setMaxBackgroundFlushes(2);
        options.getEnv().setBackgroundThreads(maxCores, Priority.HIGH);
        options.setMaxSubcompactions(2);

        options.setIncreaseParallelism(maxCores);
        options.setWriteBufferSize(64 * 1024 * 1024); // 64 MB write buffer
        options.setMaxWriteBufferNumber(4);//no. of buffer writes
        options.setMinWriteBufferNumberToMerge(2); //min no. of write buffers to merge
        options.setLevel0FileNumCompactionTrigger(10);//trigger compaction at 4 files
        options.setLevel0SlowdownWritesTrigger(20);//slow down write at 8 files
        options.setLevel0StopWritesTrigger(40); //stop write at 12 files
        options.setTargetFileSizeBase(64 * 1024 * 1024);//target file size compaction
        options.setMaxBytesForLevelBase(256 * 1024 * 1024); // max bytes level base

        options.setAllowMmapReads(true);
        options.setAllowMmapWrites(true);

//        options.setUseDirectReads(true);
//        options.setUseDirectIoForFlushAndCompaction(true);

        options.setUnorderedWrite(true);

        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(256 * 1024 * 1024); // 256MB
        options.setTableFormatConfig(tableConfig);
        tableConfig.setFilterPolicy(new BloomFilter(10));

        File dbDir = new File(filePath, dbName);
        if (isPermanent) {
            Files.deleteIfExists(dbDir.toPath());
        }

        try {
            if (canWrite) {
                if (resetDBOnStartup) {
                    if (Files.exists(dbDir.toPath()))
                        RocksDB.destroyDB(dbDir.getAbsolutePath(), options);
                }
                if (!dbDir.exists()) {
                    Files.createDirectories(dbDir.getParentFile().toPath());
                    Files.createDirectories(dbDir.getAbsoluteFile().toPath());
                }
            } else {
                System.out.println(MessageFormat.format("{0} Waiting for rockdb pre-creation on path {1}",
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")),
                        dbDir.getAbsolutePath()));
                while (!dbDir.exists()) {
                    Thread.sleep(100);
                }
                Thread.sleep(10_000);
            }
            if (canWrite) {
                return RocksDB.open(options, dbDir.getAbsolutePath());
            } else {
                return RocksDB.openReadOnly(options, dbDir.getAbsolutePath());
            }
        } catch (IOException | RocksDBException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    default boolean save(final String key,
                         final Object value) {
        return save(key.getBytes(), value);
    }

    default boolean save(final byte[] key,
                         final Object value) {
        try {
            final WriteOptions writeOptions = new WriteOptions();
            writeOptions.setDisableWAL(true);
            getDB().put(writeOptions, key, SerializationUtils.serialize(value));
        } catch (RocksDBException e) {
            return false;
        }
        return true;
    }

    default Optional<V> find(final String key) {
        return find(key.getBytes());
    }

    default Optional<V> find(final byte[] key) {
        try {
            final byte[] bytes = getDB().get(key);
            if (null != bytes) {
                return Optional.ofNullable(SerializationUtils.deserialize(bytes))
                        .map(v -> (V) v);
            }
        } catch (RocksDBException e) {
            return Optional.empty();
        }

        return Optional.empty();
    }

    default Collection<V> findMulti(final Collection<String> keys) {
        try {
            final List<byte[]> bytes = getDB().multiGetAsList(
                    keys.stream().map(String::getBytes).collect(Collectors.toList()));
            if (!bytes.isEmpty()) {
                return bytes.stream()
                        .filter(Objects::nonNull)
                        .map(m -> SerializationUtils.deserialize(m))
                        .map(v -> (V) v)
                        .collect(Collectors.toList());
            }
        } catch (RocksDBException e) {
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }


    default boolean delete(final String key) {
        try {
            getDB().delete(key.getBytes());
        } catch (RocksDBException e) {
            return false;
        }
        return true;
    }

    default Collection<V> findAll() {
        final Collection<V> values = new ArrayList<>();
        try (final RocksIterator iterator = iterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                final byte[] value = iterator.value();
                if (null != value) {
                    values.add((V) SerializationUtils.deserialize(value));
                }
            }
        }
        return values;
    }

    default long size() {
        try {
            return getDB().getLongProperty("rocksdb.estimate-num-keys");
        } catch (Exception e) {
            return 0;
        }
    }

    default RocksIterator iterator() {
        return getDB().newIterator();
    }

    default String splitKey() {
        return "|";
    }

    default List<ImmutablePair<String, V>> findByPrefix(final String key,
                                                        final boolean... fromStart) {
        return findBy(key, 1, fromStart);
    }

    default List<ImmutablePair<String, V>> findBySuffix(final String key,
                                                        final boolean... fromStart) {
        return findBy(key, 2, fromStart);
    }

    default List<ImmutablePair<String, V>> findContains(final String key,
                                                        final boolean... fromStart) {
        return findBy(key, 3, fromStart);
    }

    private List<ImmutablePair<String, V>> findBy(final String key,
                                                  final int hasType,
                                                  final boolean... fromStart) {
        final List<ImmutablePair<String, V>> values = new ArrayList<>();
        try (final RocksIterator iterator = iterator()) {
            final String targetKeyStr = new String(key.getBytes());
            final Predicate<RocksIterator> hasTypez = itr -> switch (hasType) {
                case 1 -> new String(iterator.key()).startsWith(targetKeyStr);
                case 2 -> new String(iterator.key()).endsWith(targetKeyStr);
                case 3 -> new String(iterator.key()).contains(targetKeyStr);
                default -> throw new IllegalStateException("Unexpected value: " + hasType);
            };

            if (fromStart.length > 0 && fromStart[0]) {
                for (iterator.seekToFirst(); iterator.isValid() && hasTypez.test(iterator); iterator.next()) {
                    final byte[] value = iterator.value();
                    if (null != value) {
                        values.add(ImmutablePair.of(new String(iterator.key()),
                                (V) SerializationUtils.deserialize(value)));
                    }
                }
            } else {
                for (iterator.seek(key.getBytes()); iterator.isValid() && hasTypez.test(iterator); iterator.next()) {
                    final byte[] value = iterator.value();
                    if (null != value) {
                        values.add(ImmutablePair.of(new String(iterator.key()),
                                (V) SerializationUtils.deserialize(value)));
                    }
                }
            }
        }
        return values;
    }
}
