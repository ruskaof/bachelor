package ru.itmo.rusinov.consensus.raft.core;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import raft.Raft;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MapDBDurableStateStore implements DurableStateStore {
    private DB db;
    private Map<String, byte[]> singleValuesStore;
    private Map<Long, byte[]> logEntries;

    @Override
    public void initialize(File storagePath) {
        this.db = DBMaker.fileDB(new File(storagePath, "stateStore"))
                .transactionEnable()
                .make();

        this.singleValuesStore = db.hashMap("singleValuesStore")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();

        this.logEntries = db.hashMap("logEntries")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }


    @Override
    public void saveCurrentTerm(long currentTerm) {
        singleValuesStore.put("currentTerm", Longs.toByteArray(currentTerm));
        db.commit();
    }

    @Override
    public Optional<Long> loadCurrentTerm() {
        return Optional.ofNullable(singleValuesStore.get("currentTerm"))
                .map(Longs::fromByteArray);
    }

    @Override
    public void saveVotedFor(String votedFor) {
        singleValuesStore.put("votedFor", votedFor.getBytes());
        db.commit();
    }

    @Override
    public Optional<String> loadVotedFor() {
        return Optional.ofNullable(singleValuesStore.get("votedFor"))
                .map(String::new);
    }

    @Override
    public void addLog(Long index, Raft.LogEntry logEntry) {
        logEntries.put(index, logEntry.toByteArray());
        db.commit();
    }

    @Override
    public Map<Long, Raft.LogEntry> loadLog() {
        return logEntries.entrySet()
                .stream().map((e) -> {
                    try {
                        return Map.entry(e.getKey(), Raft.LogEntry.parseFrom(e.getValue()));
                    } catch (InvalidProtocolBufferException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
