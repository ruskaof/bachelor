package ru.itmo.rusinov.consensus.kv.store.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.GetMessage;
import ru.itmo.rusinov.consensus.kv.store.api.ratis.SetMessage;
import ru.itmo.rusinov.consensus.kv.store.db.KvDatabase;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;


public class KvStateMachine extends BaseStateMachine {
    private final KvDatabase database;
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    public KvStateMachine(KvDatabase database) {
        this.database = Preconditions.assertNotNull(database, "database");
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        this.storage.init(storage);

        File stateMachineDir = storage.getStorageDir().getStateMachineDir();
        File dbFile = new File(stateMachineDir, "dbData");

        database.initialize(dbFile.toPath());
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        GetMessage message = GetMessage.valueOf(request.getContent());

        var result = database.get(message.protoMessage.getGet().getKey().toByteArray());
        if (result == null) {
            result = new byte[0];
        }
        return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result)));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        RaftProtos.LogEntryProto entry = trx.getLogEntry();
        SetMessage message = SetMessage.valueOf(entry.getStateMachineLogEntry().getLogData());
        byte[] key = message.protoMessage.getSet().getKey().toByteArray();
        byte[] value = message.protoMessage.getSet().getValue().toByteArray();
        database.put(key, value);
        return CompletableFuture.completedFuture(message);
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return this.storage;
    }
}
