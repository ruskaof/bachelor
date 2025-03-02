package ru.itmo.rusinov.consensus.kv.store.client.paxos;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import reactor.core.publisher.Mono;
import ru.itmo.rusinov.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PaxosClient {
    private final Map<String, InetSocketAddress> replicas;
    private final Map<String, Socket> streams = new ConcurrentHashMap<>();
    private final List<String> replicaIds;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public PaxosClient(Map<String, InetSocketAddress> replicas) {
        this.replicas = replicas;
        this.replicaIds = List.copyOf(replicas.keySet());
    }

    @SneakyThrows
    private synchronized void connect(String replicaId) {
        streams.computeIfAbsent(replicaId, id -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    Socket socket = new Socket(replicas.get(id).getAddress(), replicas.get(id).getPort());
                    log.info("Connected to replica {}", id);
                    return socket;
                } catch (IOException e) {
                    log.error("Connection attempt {} to {} failed: {}", attempt, id, e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
        });
    }

    public synchronized Paxos.CommandResult sendCommand(Paxos.Command paxosCommand) {
        if (replicaIds.isEmpty()) {
            log.error("No replicas available to send command");
            return null;
        }

        int index = roundRobinIndex.getAndUpdate(i -> (i + 1) % replicaIds.size());
        String selectedReplica = replicaIds.get(index);

        connect(selectedReplica);
        Socket socket = streams.get(selectedReplica);
        if (socket == null || socket.isClosed()) {
            log.error("Failed to get a valid connection for replica {}", selectedReplica);
            streams.remove(selectedReplica);
            return null;
        }

        try {
            var message = Paxos.RequestMessage.newBuilder()
                    .setCommand(paxosCommand)
                    .build();
            message.writeTo(socket.getOutputStream());
            log.info("Sent command to replica {}", selectedReplica);

            Paxos.CommandResult result = Paxos.CommandResult.parseFrom(socket.getInputStream());
            return result;
        } catch (IOException e) {
            log.error("Failed to send command to replica {}: {}", selectedReplica, e.getMessage());
            streams.remove(selectedReplica);
            return null;
        }
    }

    public Mono<Void> setStringValue(String key, String value) {
        var setMessage = Message.SetMessage.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build();
        Paxos.Command command = Paxos.Command.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder().setSet(setMessage).build().toByteString())
                .build();

        return Mono.fromCallable(() -> {
            Paxos.CommandResult result = sendCommand(command);
            if (result == null) {
                throw new RuntimeException("Failed to set value in Paxos");
            }
            return result;
        }).then();
    }

    public Mono<String> getStringValue(String key) {
        var getMessage = Message.GetMessage.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build();
        Paxos.Command command = Paxos.Command.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder().setGet(getMessage).build().toByteString())
                .build();

        return Mono.fromCallable(() -> {
            Paxos.CommandResult result = sendCommand(command);
            if (result == null) {
                throw new RuntimeException("Failed to get value from Paxos");
            }
            return result.getContent().toString();
        });
    }
}
