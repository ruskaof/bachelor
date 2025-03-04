package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;

@Service
@RequiredArgsConstructor
@Primary
public class KvStorePaxosClient implements ConsensusClient {
    private final PaxosClient client;

    public Mono<Void> setStringValue(String key, String value) {
        return client.setStringValue(key, value).then();
    }

    public Mono<String> getStringValue(String key) {
        return client.getStringValue(key);
    }
}
