package com.adel.fastcache.rocksdb;

import jakarta.annotation.PostConstruct;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Repository;

import java.io.IOException;

/**
 * @author Adel.Albediwy
 */
@Repository
public class TestRunRocksDbRepository implements RockdbBaseRepository<String>, DisposableBean {
    private final static String DB_NAME = "tradeAccountActivity_rocksdb";
    private final static String FILE_PATH = "C:\\Users\\Adel.Albediwy\\Desktop\\rocksdb_test\\";
    private RocksDB db;

    @PostConstruct
    public void init() throws IOException {
        db = initializedDB(DB_NAME, FILE_PATH, true, true, false);
    }

    @Override
    public RocksDB getDB() {
        return db;
    }

    @Override
    public Class<String> getValueClass() {
        return String.class;
    }

    @Override
    public void destroy() throws Exception {
        if (null != db && !db.isClosed()) db.close();
    }

    @Override
    public void close() throws Exception {
        if (null != db && !db.isClosed()) db.close();
    }
}
