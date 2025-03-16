package ru.itmo.rusinov.consensus.kv.store.raft;

import lombok.SneakyThrows;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.kv.store.db.KvDatabase;
import ru.itmo.rusinov.consensus.raft.core.StateMachine;

import java.io.File;
import java.util.Optional;

public class KvStoreRaftStateMachine implements StateMachine {
    private final KvDatabase database;

    public KvStoreRaftStateMachine(KvDatabase database) {
        this.database = database;
    }

    @Override
    public void initialize(File stateMachineDir) {
        File dbFile = new File(stateMachineDir, "dbData");

        database.initialize(dbFile.toPath());
    }

    @SneakyThrows
    @Override
    public byte[] applyCommand(byte[] command) {
        var protoCommand = Message.KvStoreProtoMessage.parseFrom(command);

        switch (protoCommand.getMessageCase()) {
            case GET -> {
                return handleGet(protoCommand.getGet());
            }
            case SET -> {
                return handleSet(protoCommand.getSet());
            }
        }
        return null;
    }

    private byte[] handleSet(Message.SetMessage set) {
        database.put(set.getKey().toByteArray(), set.getValue().toByteArray());
        return new byte[0];
    }

    private byte[] handleGet(Message.GetMessage get) {
        var bytes = database.get(get.getKey().toByteArray());
        return Optional.ofNullable(bytes).orElse(new byte[0]);
    }
}
