package ru.itmo.rusinov.consensus.paxos.core.environment;


import paxos.Paxos;
import paxos.Paxos.PaxosMessage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Environment extends AutoCloseable {
    void init();

    CompletableFuture<byte[]> sendMessage(String destination, PaxosMessage paxosMessage);
    void sendResponse(UUID requestId, byte[] response);

    void sendMessageToColocatedLeader(Paxos.PaxosMessage paxosMessage);
    void sendMessageToColocatedReplica(Paxos.PaxosMessage paxosMessage);
    PaxosRequest getNextAcceptorMessage();
    PaxosRequest getNextLeaderMessage();
    PaxosRequest getNextScoutMessage(UUID scoutId);
    PaxosRequest getNextCommanderMessage(UUID commanderId);
    PaxosRequest getNextReplicaMessage();
}
