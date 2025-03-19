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

        var setMessage = Message.SetMessage.newBuilder()
                .setKey(ByteString.copyFrom(key.getBytes()))
                .setValue(ByteString.copyFrom(value.getBytes()))
                .build();
        var kvStoreMessage = Message.KvStoreProtoMessage.newBuilder()
                .setSet(setMessage)
                .build();
        var raftCommand = Raft.RaftCommand.newBuilder()
                .setValue(kvStoreMessage.toByteString())
                .build();
        var clientRequest = Raft.RaftServerRequest.newBuilder()
                .setClientRequest(
                        Raft.ClientRequest.newBuilder()
                                .setRequest(raftCommand)
                )
                .build();

        log.info("Sending set to raft replica {}", leader);

        var responseBytes = environmentClient.sendMessage(clientRequest.toByteArray(), leader).get();
        var response = Raft.ClientResponse.parseFrom(responseBytes);

        if (!response.getSuggestedLeader().isEmpty()) {
            log.warn("{} not a leader. suggested leader: {}", leader, response.getSuggestedLeader());
            currentLeader.compareAndSet(leader, response.getSuggestedLeader());
            setStringValue(key, value);
        }
    }

    @SneakyThrows
    public String getStringValue(String key) {
        var leader = currentLeader.get();

        var getMessage = Message.GetMessage.newBuilder()
                .setKey(ByteString.copyFrom(key.getBytes()))
                .build();
        var kvStoreMessage = Message.KvStoreProtoMessage.newBuilder()
                .setGet(getMessage)
                .build();
        var raftCommand = Raft.RaftCommand.newBuilder()
                .setValue(kvStoreMessage.toByteString())
                .build();
        var clientRequest = Raft.RaftServerRequest.newBuilder()
                .setClientRequest(
                        Raft.ClientRequest.newBuilder()
                                .setRequest(raftCommand)
                )
                .build();

        log.info("Sending get to raft replica {}", leader);

        var responseBytes = environmentClient.sendMessage(clientRequest.toByteArray(), leader).get();
        var response = Raft.ClientResponse.parseFrom(responseBytes);

        if (!response.getSuggestedLeader().isEmpty()) {
            log.warn("{} not a leader. suggested leader: {}", leader, response.getSuggestedLeader());
            currentLeader.compareAndSet(leader, response.getSuggestedLeader());
            return getStringValue(key);
        }

        return response.getCommandResult().getValue().toStringUtf8();
    }
}