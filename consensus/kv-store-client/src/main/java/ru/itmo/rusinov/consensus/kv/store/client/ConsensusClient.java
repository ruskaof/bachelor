package ru.itmo.rusinov.consensus.kv.store.client;

public interface ConsensusClient {
    void setStringValue(String key, String value);
    String getStringValue(String key);
}
