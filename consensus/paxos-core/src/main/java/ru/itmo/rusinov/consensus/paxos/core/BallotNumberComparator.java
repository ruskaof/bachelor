package ru.itmo.rusinov.consensus.paxos.core;

import paxos.Paxos;

import java.util.Comparator;
import java.util.UUID;

public class BallotNumberComparator implements Comparator<Paxos.BallotNumber> {

    public static final Paxos.BallotNumber MIN = Paxos.BallotNumber.newBuilder()
            .setRound(Long.MIN_VALUE)
            .setLeaderId(new UUID(Long.MIN_VALUE, Long.MIN_VALUE).toString())
            .build();

    @Override
    public int compare(Paxos.BallotNumber o1, Paxos.BallotNumber o2) {
        int roundComparison = Long.compare(o1.getRound(), o2.getRound());
        if (roundComparison != 0) {
            return roundComparison;
        }
        return o1.getLeaderId().compareTo(o2.getLeaderId());
    }
}