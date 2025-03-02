package ru.itmo.rusinov.consensus.kv.store.api.ratis;

import lombok.SneakyThrows;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import ru.itmo.rusinov.Message;
import ru.itmo.rusinov.Message.KvStoreProtoMessage;

public class GetMessage implements org.apache.ratis.protocol.Message {

    public final KvStoreProtoMessage protoMessage;

    public GetMessage(KvStoreProtoMessage protoMessage) {
        this.protoMessage = protoMessage;
    }

    public GetMessage(byte[] key) {
        protoMessage = KvStoreProtoMessage.newBuilder()
                .setGet(Message.GetMessage.newBuilder().setKey(com.google.protobuf.ByteString.copyFrom(key)))
                .build();
    }

    @Override
    public ByteString getContent() {
        return ByteString.copyFrom(protoMessage.toByteArray());
    }

    @Override
    public int size() {
        return protoMessage.toByteArray().length;
    }

    @SneakyThrows
    public static GetMessage valueOf(ByteString bytes) {
        return new GetMessage(KvStoreProtoMessage.parseFrom(bytes.toByteArray()));
    }
}
