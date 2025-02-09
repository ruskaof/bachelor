package ru.itmo.rusinov.consensus.kv.store.ratis.api;

import org.apache.ratis.protocol.Message;

public interface KvStoreMessage extends Message {

    enum Type {
        GET,
        SET;
    }
}
