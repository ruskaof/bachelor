package ru.itmo.rusinov.consensus.kv.store.ratis.server;

import java.nio.file.Path;

public interface KvDatabase {
    byte[] get(byte[] key);
    void put(byte[] key, byte[] value);
    void initialize(Path databaseFilePath);
}
