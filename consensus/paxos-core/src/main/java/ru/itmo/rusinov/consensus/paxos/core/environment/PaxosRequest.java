package ru.itmo.rusinov.consensus.paxos.core.environment;

import paxos.Paxos;

import java.util.UUID;

public record PaxosRequest(
        UUID requestId,
        Paxos.PaxosMessage message
) {
}
