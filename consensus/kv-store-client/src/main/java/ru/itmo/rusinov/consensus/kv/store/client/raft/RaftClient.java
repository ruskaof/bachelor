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
    private byte[] sendRaftMessage(Raft.RaftServerRequest raftServerRequest) {

        while (true) {
            var leader = currentLeader.get();

            try {

                log.info("Sending set to raft replica {}", leader);
                var responseBytes = environmentClient.sendMessage(raftServerRequest.toByteArray(), leader).get();
                var response = Raft.ClientResponse.parseFrom(responseBytes);
                if (!response.getSuggestedLeader().isEmpty()) {
                    log.info("Suggested leader: {}", response.getSuggestedLeader());
                    currentLeader.compareAndSet(leader, response.getSuggestedLeader());
                } else {
                    return response.getCommandResult().getValue().toByteArray();
                }
            } catch (Exception e) {
                currentLeader.compareAndSet(leader, replicaIds.get((replicaIds.indexOf(leader) + 1) % replicaIds.size()));
            }
        }
    }

    @SneakyThrows
    public void setStringValue(String key, String value) {
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

        sendRaftMessage(clientRequest);
    }

    @SneakyThrows
    public String getStringValue(String key) {
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

        return new String(sendRaftMessage(clientRequest));
    }
}