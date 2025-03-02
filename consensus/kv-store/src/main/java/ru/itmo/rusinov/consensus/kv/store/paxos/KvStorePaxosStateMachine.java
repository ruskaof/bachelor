package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import paxos.Paxos;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.kv.store.db.KvDatabase;
import ru.itmo.rusinov.consensus.paxos.core.StateMachine;
import ru.itmo.rusinov.consensus.paxos.core.command.CommandResult;

import java.io.File;
import java.util.Objects;

public class KvStorePaxosStateMachine implements StateMachine {

    private final KvDatabase database;

    public KvStorePaxosStateMachine(KvDatabase database) {
        this.database = database;
    }

    @Override
    public void initialize(File stateMachineDir) {
        File dbFile = new File(stateMachineDir, "dbData");

        database.initialize(dbFile.toPath());
    }

    @SneakyThrows
    @Override
    public CommandResult applyCommand(Paxos.Command command) {
        var protoCommand = Message.KvStoreProtoMessage.parseFrom(command.getContent());

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

    private CommandResult handleSet(Message.SetMessage set) {
        database.put(set.getKey().toByteArray(), set.getValue().toByteArray());
        return () -> new byte[0];
    }

    private CommandResult handleGet(Message.GetMessage get) {
        return () -> {
            var result = database.get(get.getKey().toByteArray());
            return Objects.requireNonNullElseGet(result, () -> new byte[0]);
        };
    }
}
