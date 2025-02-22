package ru.itmo.rusinov.consensus.paxos.core;

import java.io.Serializable;
import java.util.UUID;


public record BallotNumber(long round, UUID leaderId) implements Comparable<BallotNumber>, Serializable {

    public static BallotNumber MIN = new BallotNumber(Long.MIN_VALUE, new UUID(Long.MIN_VALUE, Long.MIN_VALUE));

    @Override
    public int compareTo(BallotNumber o) {
        int valueComparison = Long.compare(this.round, o.round);
        if (valueComparison != 0) {
            return valueComparison;
        }
        return this.leaderId.compareTo(o.leaderId);
    }
}
