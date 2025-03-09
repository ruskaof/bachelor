package ru.itmo.rusinov.consensus.paxos.core;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import paxos.Paxos;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class MapDBDurableStateStore implements DurableStateStore {
    private DB db;
    private Map<String, byte[]> singleValuesStore;
    private Map<Long, byte[]> acceptedStore;

    @Override
    public void initialize(File storagePath) {
        this.db = DBMaker.fileDB(new File(storagePath, "stateStore"))
                .transactionEnable()
                .make();

        this.singleValuesStore = db.hashMap("singleValuesStore")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();

        this.acceptedStore = db.hashMap("acceptedStore")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @SneakyThrows
    @Override
    public Optional<Paxos.BallotNumber> loadAcceptorBallotNumber() {
        return Optional.ofNullable(singleValuesStore.getOrDefault("ballot", BallotNumberComparator.MIN.toByteArray()))
                .map((it) -> {
                    try {
                        return Paxos.BallotNumber.parseFrom(it);
                    } catch (InvalidProtocolBufferException ex) {
                        throw new RuntimeException(ex);
                    }
                });
    }

    @Override
    public void saveAcceptorBallotNumber(Paxos.BallotNumber ballotNumber) {
        singleValuesStore.put("ballot", ballotNumber.toByteArray());
        db.commit();
    }

    @Override
    public Map<Long, Paxos.Pvalue> loadAcceptedValues() {
        return acceptedStore.entrySet()
                .stream().map((e) -> {
                    try {
                        return Map.entry(e.getKey(), Paxos.Pvalue.parseFrom(e.getValue()));
                    } catch (InvalidProtocolBufferException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void saveAcceptedValue(long slot, Paxos.Pvalue pvalue) {
        acceptedStore.put(slot, pvalue.toByteArray());
        db.commit();
    }

    @Override
    public void clearAcceptedBelowSlot(long slot) {
        acceptedStore.entrySet().removeIf((e) -> e.getKey() < slot);
        db.commit();
    }

    @Override
    public void saveLastAppliedSlot(long slot) {
        singleValuesStore.put("lastAppliedSlot", Longs.toByteArray(slot));
        db.commit();
    }

    @Override
    public Optional<Long> loadLastAppliedSlot() {
        return Optional.ofNullable(singleValuesStore.get("lastAppliedSlot"))
                .map(Longs::fromByteArray);
    }
}

