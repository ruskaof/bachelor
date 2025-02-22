package ru.itmo.rusinov.consensus.paxos.core;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;

public record Pvalue(
        BallotNumber ballotNumber,
        long slotNumber,
        Command command
) {
}
