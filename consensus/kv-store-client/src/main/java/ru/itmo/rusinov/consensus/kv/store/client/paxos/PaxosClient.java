package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.common.EnvironmentClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PaxosClient {
    private final List<String> replicaIds;
    private final EnvironmentClient environmentClient;
    private final AtomicInteger replicaIndex = new AtomicInteger(0);

    public PaxosClient(List<String> replicaIds, EnvironmentClient environmentClient) {
        this.replicaIds = replicaIds;
        this.environmentClient = environmentClient;
    }

    private String selectReplica() {
        int index = replicaIndex.getAndUpdate(i -> (i + 1) % replicaIds.size());
        return replicaIds.get(index);
    }

    private CompletableFuture<Paxos.CommandResult> sendCommand(Paxos.ClientCommand paxosCommand) {
        return sendCommandToReplica(paxosCommand, 0);
    }

    private CompletableFuture<Paxos.CommandResult> sendCommandToReplica(Paxos.ClientCommand paxosCommand, int attempt) {
        if (attempt >= replicaIds.size()) {
            return CompletableFuture.failedFuture(new RuntimeException("All replicas failed"));
        }

        String replica = selectReplica();
        log.info("Sending request to replica: {}", replica);

        var message = Paxos.PaxosMessage.newBuilder()
                .setRequest(
                        Paxos.RequestMessage.newBuilder()
                                .setCommand(paxosCommand)
                                .build()
                )
                .build();

        return environmentClient.sendMessage(message.toByteArray(), replica)
                .exceptionally(ex -> {
                    log.error("Exception while contacting replica {}: {}", replica, ex.getMessage());
                    return sendCommandToReplica(paxosCommand, attempt + 1).join().toByteArray();
                })
                .thenApply((b) -> {
                    try {
                        return Paxos.CommandResult.parseFrom(b);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public Mono<Void> setStringValue(String key, String value) {
        Paxos.ClientCommand command = Paxos.ClientCommand.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setSet(Message.SetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8(value))
                                .build())
                        .build()
                        .toByteString())
                .build();

        return Mono.fromFuture(sendCommand(command)).then();
    }

    public Mono<String> getStringValue(String key) {
        Paxos.ClientCommand command = Paxos.ClientCommand.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setGet(Message.GetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .build())
                        .build()
                        .toByteString())
                .build();

        return Mono.fromFuture(sendCommand(command).thenApply(f -> f.getContent().toStringUtf8()));
    }
}