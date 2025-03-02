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
    private volatile CodedOutputStream out;

    public SocketManager(InetSocketAddress address) {
        this.address = address;
        connect();
    }

    @SneakyThrows
    private synchronized void connect() {
        close();
        for (int attempt = 1; ; attempt++) {
            try {
                socket = new Socket(address.getHostName(), address.getPort());
                out = CodedOutputStream.newInstance(socket.getOutputStream());
                log.info("Connected to {}", address);
                return;
            } catch (IOException e) {
                log.error("Connection attempt {} to {} failed: {}", attempt, address, e.getMessage());
                Thread.sleep(1000);
            }
        }
    }

    public synchronized void sendMessage(Paxos.PaxosMessage paxosMessage) {
        if (socket == null || socket.isClosed()) {
            connect();
        }
        if (socket != null && out != null) {
            try {
                out.writeMessageNoTag(paxosMessage);
                out.flush();
            } catch (IOException e) {
                log.error("Error sending message:", e);
                connect();
            }
        }
    }

    @SneakyThrows
    private synchronized void close() {
        if (socket != null) socket.close();
    }
}