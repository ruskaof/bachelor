package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;

import java.util.UUID;

public record ProposeMessage(
        UUID src,
        long slotNumber,
        Command command
) implements PaxosMessage {
}
