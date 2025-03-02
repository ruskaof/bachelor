package ru.itmo.rusinov.consensus.kv.store.client;

import reactor.core.publisher.Mono;

public interface ConsensusClient {
    Mono<Void> setStringValue(String key, String value);
    Mono<String> getStringValue(String key);
}
