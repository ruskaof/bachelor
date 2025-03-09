package ru.itmo.rusinov.consensus.paxos.core;

import paxos.Paxos;

public class LogUtils {

    public static String ballotNumberStr(Paxos.BallotNumber ballotNumber) {
        return "ballot[" + ballotNumber.getRound() + ", " + ballotNumber.getLeaderId();
    }
}
