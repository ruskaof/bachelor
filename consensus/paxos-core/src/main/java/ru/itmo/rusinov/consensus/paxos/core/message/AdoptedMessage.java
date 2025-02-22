package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.BallotNumber;
import ru.itmo.rusinov.consensus.paxos.core.Pvalue;

import java.util.Set;
import java.util.UUID;

public record AdoptedMessage(
        UUID src,
        BallotNumber ballotNumber,
        Set<Pvalue> accepted
) implements PaxosMessage {
}
