package ru.itmo.rusinov.consensus.kv.store.client.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvGetStringResponse;
import ru.itmo.rusinov.consensus.kv.store.client.model.KvSetStringRequest;

@RestController
@RequiredArgsConstructor
public class KvStoreController {

    private final ConsensusClient consensusClient;

    @GetMapping("/store/{key}")
    public KvGetStringResponse get(@PathVariable("key") String key) {
        return new KvGetStringResponse(consensusClient.getStringValue(key));
    }

    @PutMapping("/store/{key}")
    public void set(@PathVariable("key") String key, @RequestBody KvSetStringRequest request) {
        consensusClient.setStringValue(key, request.value());
    }
}
