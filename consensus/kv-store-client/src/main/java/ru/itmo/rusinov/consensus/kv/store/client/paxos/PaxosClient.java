package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import raft.Raft;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.common.EnvironmentClient;

import java.util.List;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class PaxosClient {
    private final List<String> replicaIds;
    private final EnvironmentClient environmentClient;
    private final AtomicReference<String> currentLeader;

    public PaxosClient(List<String> replicaIds, EnvironmentClient environmentClient) {
        this.replicaIds = replicaIds;
        this.environmentClient = environmentClient;
        this.currentLeader = new AtomicReference<>(replicaIds.getFirst());
    }

    @SneakyThrows
    private byte[] sendPaxosMessage(Paxos.ClientCommand clientCommand) {
        var request = Paxos.PaxosMessage.newBuilder()
                .setRequest(
                        Paxos.RequestMessage.newBuilder()
                                .setCommand(clientCommand)
                                .build()
                )
                .build();

        while (true) {
            var leader = currentLeader.get();

            try {

                log.info("Sending set to paxos replica {}", leader);
                var responseBytes = environmentClient.sendMessage(request.toByteArray(), leader).get();
                var response = Paxos.CommandResult.parseFrom(responseBytes);
                if (!response.getSuggestedLeader().isEmpty()) {
                    log.info("Suggested leader: {}", response.getSuggestedLeader());
                    currentLeader.compareAndSet(leader, response.getSuggestedLeader());
                } else {
                    return response.getContent().toByteArray();
                }
            } catch (Exception e) {
                log.error("Error sending request", e);
                currentLeader.compareAndSet(leader, replicaIds.get((replicaIds.indexOf(leader) + 1) % replicaIds.size()));
            }
        }
    }

    @SneakyThrows
    public void setStringValue(String key, String value) {
        var command = Paxos.ClientCommand.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setSet(Message.SetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8(value))
                                .build())
                        .build()
                        .toByteString())
                        .build();

        sendPaxosMessage(command);
    }

    @SneakyThrows
    public String getStringValue(String key) {
        Paxos.ClientCommand command = Paxos.ClientCommand.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setGet(Message.GetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .build())
                        .build()
                        .toByteString())
                .build();

        return new String(sendPaxosMessage(command));
    }
}