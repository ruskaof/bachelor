package ru.itmo.rusinov.consensus.paxos.core.message;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;

import java.util.UUID;

public record RequestMessage(
        UUID src,
        Command command
) implements PaxosMessage {
}
