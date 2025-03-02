package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.BallotNumber;

import java.util.UUID;

public record P2bMessage(
        UUID src,
        BallotNumber ballotNumber,
        long slotNumber,
        UUID commanderId
) implements PaxosMessage {
}
