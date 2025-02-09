package ru.itmo.rusinov.consensus.kv.store.ratis.api;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public record SetMessage(byte[] key, byte[] value) implements Message {

    // Constructor

    @Override
    public ByteString getContent() {
        ByteString keyByteString = ByteString.copyFrom(key);
        ByteString valueByteString = ByteString.copyFrom(value);

        // Prepend the key size (4 bytes) to the message content
        ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + value.length);
        buffer.putInt(key.length);  // Prepend key size (4 bytes)
        buffer.put(key);            // Append key
        buffer.put(value);          // Append value

        return ByteString.copyFrom(buffer.array());
    }

    @Override
    public int size() {
        return 4 + key.length + value.length;
    }

    public static SetMessage valueOf(ByteString bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
        int keySize = buffer.getInt();

        byte[] key = new byte[keySize];
        buffer.get(key);  // Get the key bytes

        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);  // Get the value bytes

        return new SetMessage(key, value);
    }
}
