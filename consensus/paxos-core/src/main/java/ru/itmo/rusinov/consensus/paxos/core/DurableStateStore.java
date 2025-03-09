package ru.itmo.rusinov.consensus.paxos.core;

import paxos.Paxos;

import java.io.File;
import java.util.Map;
import java.util.Optional;

public interface DurableStateStore {
    void initialize(File storagePath);

    Optional<Paxos.BallotNumber> loadAcceptorBallotNumber();
    void saveAcceptorBallotNumber(Paxos.BallotNumber ballotNumber);
    Map<Long, Paxos.Pvalue> loadAcceptedValues();
    void saveAcceptedValue(long slot, Paxos.Pvalue pvalue);

    void clearAcceptedBelowSlot(long slot);

    void saveLastAppliedSlot(long slot);
    Optional<Long> loadLastAppliedSlot();
}
