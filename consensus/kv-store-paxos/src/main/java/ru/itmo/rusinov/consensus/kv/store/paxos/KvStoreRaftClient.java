package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KvStoreRaftClient {
    private final RaftClient client;

    public void setStringValue(String key, String value) {
        client.setStringValue(key, value);
    }

    public String getStringValue(String key) {
        return client.getStringValue(key);
    }
}
