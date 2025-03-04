package ru.itmo.rusinov.consensus.kv.store.paxos;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import paxos.Paxos;
import paxos.Paxos.CommandResult;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.consensus.kv.store.db.KvDatabase;
import ru.itmo.rusinov.consensus.paxos.core.StateMachine;

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
        return CommandResult.getDefaultInstance();
    }

    private CommandResult handleGet(Message.GetMessage get) {
        var bytes = database.get(get.getKey().toByteArray());
        var builder = CommandResult.newBuilder();
        if (Objects.nonNull(bytes)) {
            builder.setContent(ByteString.copyFrom(bytes));
        }

        return builder.build();
    }
}
