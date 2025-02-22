package ru.itmo.rusinov.consensus.paxos.core.config;

import java.util.Set;
import java.util.UUID;

public record Config(
        Set<UUID> leaders,
        Set<UUID> replicas,
        Set<UUID> acceptors
) {
}
