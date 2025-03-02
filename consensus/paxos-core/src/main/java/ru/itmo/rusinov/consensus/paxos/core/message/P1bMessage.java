package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.BallotNumber;
import ru.itmo.rusinov.consensus.paxos.core.Pvalue;

import java.util.Set;
import java.util.UUID;

public record P1bMessage(
        UUID src,
        BallotNumber ballotNumber,
        Set<Pvalue> accepted,
        UUID scoutId
) implements PaxosMessage {
}
