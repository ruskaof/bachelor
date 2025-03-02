package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import lombok.RequiredArgsConstructor;
import org.apache.ratis.client.RaftClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.GetMessage;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.SetMessage;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;

import java.nio.charset.StandardCharsets;

@Service
@RequiredArgsConstructor
public class KvStorePaxosClient implements ConsensusClient {
    private final RaftClient client;

    public Mono<Void> setStringValue(String key, String value) {
        var message = new SetMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));

        return Mono.fromFuture(client.async().send(message)).then();
    }

    public Mono<String> getStringValue(String key) {
        var message = new GetMessage(key.getBytes(StandardCharsets.UTF_8));

        return Mono.fromFuture(client.async().sendReadOnly(message))
                .map((r) -> r.getMessage().getContent().toStringUtf8());
    }
}
