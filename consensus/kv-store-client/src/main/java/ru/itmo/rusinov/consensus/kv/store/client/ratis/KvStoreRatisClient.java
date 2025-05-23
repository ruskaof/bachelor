package ru.itmo.rusinov.consensus.kv.store.client.ratis;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.ratis.client.RaftClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.GetMessage;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.SetMessage;
import ru.itmo.rusinov.consensus.kv.store.client.ConsensusClient;

import java.nio.charset.StandardCharsets;

@Service
@ConditionalOnProperty(name = "consensus.protocol", havingValue = "ratis")
@RequiredArgsConstructor
public class KvStoreRatisClient implements ConsensusClient {
    private final RaftClient client;

    @SneakyThrows
    public void setStringValue(String key, String value) {
        var message = new SetMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));

        client.io().send(message);
    }

    @SneakyThrows
    public String getStringValue(String key) {
        var message = new GetMessage(key.getBytes(StandardCharsets.UTF_8));

        return client.io().sendReadOnly(message).getMessage().getContent().toStringUtf8();
    }
}
