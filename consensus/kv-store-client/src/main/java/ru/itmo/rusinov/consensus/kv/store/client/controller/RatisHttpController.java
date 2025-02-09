package ru.itmo.rusinov.consensus.kv.store.client.controller;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvGetStringResponse;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvSetStringRequest;
import ru.itmo.rusinov.consensus.kv.store.client.ratis.KvStoreRatisClient;

@RestController
@RequiredArgsConstructor
public class RatisHttpController {

    private final KvStoreRatisClient kvStoreRatisClient;

    @GetMapping("/store/{key}")
    Mono<KvGetStringResponse> get(@PathVariable("key") String key) {
        return kvStoreRatisClient.getStringValue(key).map(KvGetStringResponse::new);
    }

    @PutMapping("/store/{key}")
    Mono<Void> set(@PathVariable("key") String key, @RequestBody KvSetStringRequest request) {
        return kvStoreRatisClient.setStringValue(key, request.value());
    }
}
