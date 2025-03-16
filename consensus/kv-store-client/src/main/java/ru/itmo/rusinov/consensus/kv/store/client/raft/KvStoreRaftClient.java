package ru.itmo.rusinov.consensus.kv.store.client.raft;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "consensus.protocol", havingValue = "raft")
public class KvStoreRaftClient implements ConsensusClient {
    private final RaftClient client;

    public void setStringValue(String key, String value) {
        client.setStringValue(key, value);
    }

    public String getStringValue(String key) {
        return client.getStringValue(key);
    }
}
