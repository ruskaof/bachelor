package ru.itmo.rusinov.consensus.raft.core;

import raft.Raft;

import java.io.File;
import java.util.Map;
import java.util.Optional;

public interface DurableStateStore {
    void initialize(File storagePath);

    void saveCurrentTerm(long currentTerm);
    Optional<Long> loadCurrentTerm();

    void saveVotedFor(String votedFor);
    Optional<String> loadVotedFor();

    void addLog(Long index, Raft.LogEntry logEntry);
    Map<Long, Raft.LogEntry> loadLog();
}
