package ru.itmo.rusinov.consensus.kv.store.client.raft;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import raft.Raft;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.common.EnvironmentClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class RaftClient {

    private final List<String> replicaIds;
    private final EnvironmentClient environmentClient;

    private final AtomicReference<String> currentLeader;

    public RaftClient(List<String> replicaIds, EnvironmentClient environmentClient) {
        this.replicaIds = replicaIds;
        this.environmentClient = environmentClient;
        this.currentLeader = new AtomicReference<>(replicaIds.getFirst());
    }

    @SneakyThrows
    public void setStringValue(String key, String value) {
        var leader = currentLeader.get();

        var commandBuilder = Message.KvStoreProtoMessage.newBuilder();
        commandBuilder.getSetBuilder().setValue(ByteString.copyFrom(value.getBytes()));
        commandBuilder.getSetBuilder().setKey(ByteString.copyFrom(key.getBytes()));

        var messageBuilder = Raft.ClientRequest.newBuilder();
        messageBuilder.getRequestBuilder().setValue(ByteString.copyFrom(commandBuilder.build().toByteArray()));


        var responseBytes = environmentClient.sendMessage(messageBuilder.build().toByteArray(), leader).get();
        var response = Raft.ClientResponse.parseFrom(responseBytes);

        if (!response.getSuggestedLeader().isEmpty()) {
            currentLeader.compareAndSet(leader, response.getSuggestedLeader());
            setStringValue(key, value);
        }
    }

    @SneakyThrows
    public String getStringValue(String key) {
        var leader = currentLeader.get();

        var commandBuilder = Message.KvStoreProtoMessage.newBuilder();
        commandBuilder.getGetBuilder().setKey(ByteString.copyFrom(key.getBytes()));

        var messageBuilder = Raft.ClientRequest.newBuilder();
        messageBuilder.getRequestBuilder().setValue(ByteString.copyFrom(commandBuilder.build().toByteArray()));

        var responseBytes = environmentClient.sendMessage(messageBuilder.build().toByteArray(), leader).get();
        var response = Raft.ClientResponse.parseFrom(responseBytes);

        if (!response.getSuggestedLeader().isEmpty()) {
            currentLeader.compareAndSet(leader, response.getSuggestedLeader());
            return getStringValue(key);
        }

        return response.getCommandResult().getValue().toStringUtf8();
    }
}