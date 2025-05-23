package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "consensus.protocol", havingValue = "paxos")
public class KvStorePaxosClient implements ConsensusClient {
    private final PaxosClient client;

    public void setStringValue(String key, String value) {
        client.setStringValue(key, value);
    }

    public String getStringValue(String key) {
        return client.getStringValue(key);
    }
}
