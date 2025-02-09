package ru.itmo.rusinov.consensus.kv.store.ratis.server;

import lombok.extern.slf4j.Slf4j;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.nio.file.Path;

@Slf4j
public class MapDbKvDatabase implements KvDatabase {

    private DB db;
    private HTreeMap<byte[], byte[]> map;

    @Override
    public void initialize(Path databaseFilePath) {
        log.info("Initializing mapDB storage on path {}", databaseFilePath);
        // Initialize the database and create or open the map
        db = DBMaker.fileDB(databaseFilePath.toFile())
                .fileMmapEnable() // Enable memory-mapped files for better performance
                .make();

        // Create or open a map (HTreeMap) with byte array keys and values
        map = db.hashMap("myMap")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @Override
    public byte[] get(byte[] key) {
        // Retrieve the value associated with the key
        return map.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        // Store the key-value pair in the map
        map.put(key, value);
        // Commit the transaction to ensure data is written to disk
        db.commit();
    }

    // Optional: Close the database when done to release resources
    public void close() {
        if (db != null && !db.isClosed()) {
            db.close();
        }
    }
}