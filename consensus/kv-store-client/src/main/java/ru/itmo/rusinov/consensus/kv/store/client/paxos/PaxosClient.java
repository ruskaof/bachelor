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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    private synchronized Socket getOrCreateConnection(String replicaId) {
        Socket socket = streams.get(replicaId);
        if (socket != null && !socket.isClosed()) {
            return socket;
        }

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                socket = new Socket();
                socket.connect(replicas.get(replicaId), 1000);
                streams.put(replicaId, socket);
                log.info("Connected to replica {}", replicaId);
                return socket;
            } catch (IOException e) {
                log.error("Connection attempt {} to {} failed: {}", attempt, replicaId, e.getMessage());
                Thread.sleep(1000);
            }
        }
        streams.remove(replicaId);
        return null;
    }

    @SneakyThrows
    private Paxos.CommandResult sendCommandToReplica(String replicaId, Paxos.Command paxosCommand) {
        Socket socket = getOrCreateConnection(replicaId);
        if (socket == null) {
            log.error("Failed to connect to replica {}", replicaId);
            return null;
        }
        try {
            Paxos.RequestMessage request = Paxos.RequestMessage.newBuilder()
                    .setCommand(paxosCommand)
                    .build();
            Paxos.PaxosMessage requestMessage = Paxos.PaxosMessage.newBuilder()
                    .setRequest(request)
                    .build();
            requestMessage.writeDelimitedTo(socket.getOutputStream());
            log.info("Sent command to replica {}", replicaId);
            return Paxos.CommandResult.parseDelimitedFrom(socket.getInputStream());
        } catch (IOException e) {
            log.error("Failed to send command to replica {}: {}", replicaId, e.getMessage());
            streams.get(replicaId).close();
            streams.remove(replicaId);
            return null;
        }
    }

    public synchronized Paxos.CommandResult sendCommand(Paxos.Command paxosCommand) {
        if (replicaIds.isEmpty()) {
            log.error("No replicas available to send command");
            return null;
        }

        for (int i = 0; i < replicaIds.size(); i++) {
            int index = roundRobinIndex.getAndUpdate(n -> (n + 1) % replicaIds.size());
            String selectedReplica = replicaIds.get(index);
            Paxos.CommandResult result = sendCommandToReplica(selectedReplica, paxosCommand);
            if (result != null) {
                return result;
            }
        }
        log.error("All replicas are unavailable");
        return null;
    }

    public Mono<Void> setStringValue(String key, String value) {
        Paxos.Command command = Paxos.Command.newBuilder()
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setSet(Message.SetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8(value))
                                .build())
                        .build()
                        .toByteString())
                .build();

        return Mono.fromCallable(() -> {
            if (sendCommand(command) == null) {
                throw new RuntimeException("Failed to set value in Paxos");
            }
            return null;
        }).then();
    }

    public Mono<String> getStringValue(String key) {
        Paxos.Command command = Paxos.Command.newBuilder()
                .setClientId(UUID.randomUUID().toString())
                .setContent(Message.KvStoreProtoMessage.newBuilder()
                        .setGet(Message.GetMessage.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .build())
                        .build()
                        .toByteString())
                .build();

        return Mono.fromCallable(() -> {
            Paxos.CommandResult result = sendCommand(command);
            if (result == null) {
                throw new RuntimeException("Failed to get value from Paxos");
            }
            return result.getContent().toStringUtf8();
        });
    }
}
