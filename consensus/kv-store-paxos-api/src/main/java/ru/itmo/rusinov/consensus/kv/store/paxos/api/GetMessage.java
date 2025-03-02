package ru.itmo.rusinov.consensus.kv.store.paxos.api;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;

import java.nio.ByteBuffer;

public record GetMessage(byte[] key) implements Command {

    @Override
    public byte[] getContent() {
        var bb = ByteBuffer.allocate(key.length + 4);
        bb.putInt(0); // 0 for get
        bb.put(key);
        return key;
    }
}

