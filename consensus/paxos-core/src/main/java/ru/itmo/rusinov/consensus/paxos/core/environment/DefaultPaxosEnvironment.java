package ru.itmo.rusinov.consensus.paxos.core.environment;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import ru.itmo.rusinov.consensus.common.DistributedServer;
import ru.itmo.rusinov.consensus.common.EnvironmentClient;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

import static paxos.Paxos.PaxosMessage.MessageCase.*;

@Slf4j
public class DefaultPaxosEnvironment implements Environment {
    private final DistributedServer distributedServer;
    private final EnvironmentClient environmentClient;
    private final String replicaId;

    private final ConcurrentHashMap<UUID, CompletableFuture<byte[]>> requests = new ConcurrentHashMap<>();

    private final BlockingQueue<PaxosRequest> acceptorQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosRequest> replicaQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosRequest> leaderQueue = new LinkedBlockingQueue<>();

    private final ConcurrentHashMap<String, LinkedBlockingQueue<PaxosRequest>> commanderQueues =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LinkedBlockingQueue<PaxosRequest>> scoutQueues =
            new ConcurrentHashMap<>();

    public DefaultPaxosEnvironment(DistributedServer distributedServer, EnvironmentClient environmentClient, String replicaId) {
        this.distributedServer = distributedServer;
        this.environmentClient = environmentClient;
        this.replicaId = replicaId;
    }

    @SneakyThrows
    @Override
    public void sendMessageToColocatedLeader(Paxos.PaxosMessage paxosMessage) {
        leaderQueue.put(new PaxosRequest(null, paxosMessage));
    }

    @SneakyThrows
    @Override
    public void sendMessageToColocatedReplica(Paxos.PaxosMessage paxosMessage) {
        replicaQueue.put(new PaxosRequest(null, paxosMessage));
    }

    @SneakyThrows
    @Override
    public CompletableFuture<byte[]> sendMessage(String destination, Paxos.PaxosMessage paxosMessage) {
        if (replicaId.equals(destination)) {
            putMessageToQueues(new PaxosRequest(null, paxosMessage));
            return CompletableFuture.completedFuture(new byte[0]);
        }

        return environmentClient.sendMessage(paxosMessage.toByteArray(), destination)
                .whenComplete((r, e) -> {
                    if (Objects.nonNull(e)) {
                        log.error("Could not send message {} to {}", paxosMessage.getMessageCase(), destination, e);
                    }
                });
    }

    @Override
    public void sendResponse(UUID requestId, byte[] response) {
        var result = Optional.ofNullable(response).orElse(new byte[0]);

        requests.remove(requestId).complete(result);
    }

    @SneakyThrows
    @Override
    public PaxosRequest getNextAcceptorMessage() {
        return acceptorQueue.take();
    }

    @SneakyThrows
    @Override
    public PaxosRequest getNextLeaderMessage() {
        return leaderQueue.take();
    }

    @SneakyThrows
    @Override
    public PaxosRequest getNextScoutMessage(UUID scoutId) {
        scoutQueues.putIfAbsent(scoutId.toString(), new LinkedBlockingQueue<>());

        return scoutQueues.get(scoutId.toString()).take();
    }

    @SneakyThrows
    @Override
    public PaxosRequest getNextCommanderMessage(UUID commanderId) {
        commanderQueues.putIfAbsent(commanderId.toString(), new LinkedBlockingQueue<>());

        return commanderQueues.get(commanderId.toString()).take();
    }

    @SneakyThrows
    @Override
    public PaxosRequest getNextReplicaMessage() {
        return replicaQueue.take();
    }

    @Override
    public void init() {
        distributedServer.setRequestHandler(this::handleMessage);
        distributedServer.initialize();
        environmentClient.initialize();
    }

    @Override
    public void close() throws Exception {
        distributedServer.close();
    }

    @SneakyThrows
    private CompletableFuture<byte[]> handleMessage(byte[] bytes) {
        var paxosMessage = Paxos.PaxosMessage.parseFrom(bytes);

        if (paxosMessage.getMessageCase().equals(Paxos.PaxosMessage.MessageCase.PING)) {
            return CompletableFuture.completedFuture(new byte[0]);
        }

        var requestId = UUID.randomUUID();
        var future = new CompletableFuture<byte[]>();
        requests.put(requestId, future);
        var request = new PaxosRequest(requestId, paxosMessage);

        if (!messagesWithResponse.contains(request.message().getMessageCase())) {
            sendResponse(requestId, new byte[0]);
        }

        putMessageToQueues(request);
        return future;
    }

    private void putMessageToQueues(PaxosRequest paxosRequest) throws InterruptedException {
        switch (paxosRequest.message().getMessageCase()) {

            case REQUEST, DECISION -> replicaQueue.put(paxosRequest);

            case PROPOSE, ADOPTED, PREEMPTED -> leaderQueue.put(paxosRequest);

            case P1B -> scoutQueues
                    .computeIfAbsent(paxosRequest.message().getP1B().getScoutId(),
                            (uuid -> new LinkedBlockingQueue<>())).put(paxosRequest);
            case P2B -> commanderQueues
                    .computeIfAbsent(paxosRequest.message().getP2B().getCommanderId(),
                            (uuid -> new LinkedBlockingQueue<>())).put(paxosRequest);

            case P1A, P2A -> acceptorQueue.put(paxosRequest);

            default -> throw new IllegalStateException("Unexpected value: " + paxosRequest);
        }
    }

    private static final Set<Paxos.PaxosMessage.MessageCase> messagesWithResponse = Set.of(REQUEST);
}
