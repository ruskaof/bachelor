package ru.itmo.rusinov.consensus.kv.store.client.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvGetStringResponse;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvSetStringRequest;

@RestController
@RequiredArgsConstructor
public class RatisHttpController {

    private final ConsensusClient consensusClient;

    @GetMapping("/store/{key}")
    Mono<KvGetStringResponse> get(@PathVariable("key") String key) {
        return consensusClient.getStringValue(key).map(KvGetStringResponse::new);
    }

    @PutMapping("/store/{key}")
    Mono<Void> set(@PathVariable("key") String key, @RequestBody KvSetStringRequest request) {
        return consensusClient.setStringValue(key, request.value());
    }
}
