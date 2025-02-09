package ru.itmo.rusinov.consensus.kv.store.ratis.server;

import java.util.Arrays;

public final class ByteArrayWrapper {
    private final byte[] value;

    public ByteArrayWrapper(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Byte array cannot be null");
        }
        this.value = Arrays.copyOf(value, value.length);
    }

    public byte[] getValue() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ByteArrayWrapper other = (ByteArrayWrapper) obj;
        return Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "ByteArrayWrapper{" + "value=" + Arrays.toString(value) + '}';
    }
}