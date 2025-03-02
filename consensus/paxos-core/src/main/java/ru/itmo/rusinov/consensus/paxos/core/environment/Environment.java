package ru.itmo.rusinov.consensus.paxos.core.environment;

import ru.itmo.rusinov.consensus.paxos.core.message.PaxosMessage;

import java.util.UUID;

public interface Environment {

    void sendMessage(UUID destination, PaxosMessage paxosMessage);
    PaxosMessage getNextAcceptorMessage();
    PaxosMessage getNextLeaderMessage();
    PaxosMessage getNextScoutMessage(UUID scoutId);
    PaxosMessage getNextCommanderMessage(UUID commanderId);
    PaxosMessage getNextReplicaMessage();
}
