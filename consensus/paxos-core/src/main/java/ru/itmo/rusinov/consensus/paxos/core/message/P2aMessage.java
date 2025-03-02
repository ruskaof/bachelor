package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.BallotNumber;
import ru.itmo.rusinov.consensus.paxos.core.command.Command;

import java.util.UUID;

public record P2aMessage(
        UUID src,
        BallotNumber ballotNumber,
        long slotNumber,
        Command command,
        UUID commanderId
) implements PaxosMessage {
}
