package ru.itmo.rusinov.consensus.kv.store.api.ratis;

import lombok.SneakyThrows;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import ru.itmo.rusinov.Message.KvStoreProtoMessage;

public class SetMessage implements org.apache.ratis.protocol.Message {

    public final KvStoreProtoMessage protoMessage;

    public SetMessage(KvStoreProtoMessage protoMessage) {
        this.protoMessage = protoMessage;
    }

    public SetMessage(byte[] key, byte[] value) {
        protoMessage = KvStoreProtoMessage.newBuilder()
                .setSet(ru.itmo.rusinov.Message.SetMessage.newBuilder().setKey(com.google.protobuf.ByteString.copyFrom(key)).setValue(com.google.protobuf.ByteString.copyFrom(value)))
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
    public static SetMessage valueOf(ByteString bytes) {
        return new SetMessage(KvStoreProtoMessage.parseFrom(bytes.toByteArray()));
    }
}
