package ru.itmo.rusinov.consensus.paxos.core.environment;


import paxos.Paxos;
import paxos.Paxos.PaxosMessage;

import java.util.UUID;

public interface Environment {

    void sendMessage(String destination, PaxosMessage paxosMessage);
    void sendResponse(String clientId, Paxos.CommandResult response);

    PaxosMessage getNextAcceptorMessage();
    PaxosMessage getNextLeaderMessage();
    PaxosMessage getNextScoutMessage(UUID scoutId);
    PaxosMessage getNextCommanderMessage(UUID commanderId);
    PaxosMessage getNextReplicaMessage();
}
