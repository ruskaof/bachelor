package ru.itmo.rusinov.consensus.paxos.core.command;

import ru.itmo.rusinov.consensus.paxos.core.config.Config;

public record ReconfigCommand(
        Config config
) implements Command {
}
