package ru.itmo.rusinov.consensus.paxos.core.environment.tcp;

import com.google.protobuf.CodedOutputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
public class SocketManager {
    private final InetSocketAddress address;
    private volatile Socket socket;

    public SocketManager(InetSocketAddress address) {
        this.address = address;
        connect();
    }

    @SneakyThrows
    private synchronized void connect() {
        close();
        try {
            socket = new Socket();
            socket.connect(address, 1000);
            log.info("Connected to {}", address);
        } catch (IOException e) {
            log.warn("Connection to {} failed: {} {}", address, e.getClass().getCanonicalName(), e.getMessage());
        }
    }

    @SneakyThrows
    public synchronized void sendMessage(Paxos.PaxosMessage paxosMessage) {
        if (socket == null || socket.isClosed()) {
            connect();
        }
        if (socket != null) {
            try {
                paxosMessage.writeDelimitedTo(socket.getOutputStream());
            } catch (IOException e) {
                log.warn("Could not send message to {}: {} {}",
                        this.address, e.getClass().getCanonicalName(), e.getMessage());
                socket.close();
                socket = null;
            }
        }
    }

    @SneakyThrows
    private synchronized void close() {
        if (socket != null) socket.close();
    }
}