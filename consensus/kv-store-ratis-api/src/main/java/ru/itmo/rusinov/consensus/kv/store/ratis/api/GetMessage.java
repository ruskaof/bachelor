package ru.itmo.rusinov.consensus.kv.store.ratis.api;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public record GetMessage(byte[] key) implements KvStoreMessage {

    @Override
    public ByteString getContent() {
        return ByteString.copyFrom(key);
    }

    @Override
    public int size() {
        return key.length;
    }
}
