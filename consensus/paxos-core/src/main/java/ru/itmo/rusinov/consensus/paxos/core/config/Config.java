package ru.itmo.rusinov.consensus.paxos.core.config;

import java.util.Set;

public record Config(
        Set<String> replicas
) {
}
