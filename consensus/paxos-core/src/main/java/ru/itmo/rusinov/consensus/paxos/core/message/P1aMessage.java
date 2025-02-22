package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.BallotNumber;

import java.util.UUID;

public record P1aMessage(
        UUID src,
        BallotNumber ballotNumber
) implements PaxosMessage {
}
