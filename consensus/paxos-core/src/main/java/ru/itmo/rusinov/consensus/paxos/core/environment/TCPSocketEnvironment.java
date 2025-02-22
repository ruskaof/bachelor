package ru.itmo.rusinov.consensus.paxos.core.environment;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.message.PaxosMessage;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class TCPSocketEnvironment implements Environment {
    private final Map<UUID, InetSocketAddress> destinationMap = new ConcurrentHashMap<>();
    private final Map<UUID, SocketManager> socketManagers = new ConcurrentHashMap<>();
    private final BlockingQueue<PaxosMessage> messageQueue = new LinkedBlockingQueue<>();
    private final int serverPort;
    private ServerSocket serverSocket;

    public TCPSocketEnvironment(int serverPort) {
        this.serverPort = serverPort;
        startServer();
    }

    public void addDestination(UUID uuid, String host, int port) {
        destinationMap.put(uuid, new InetSocketAddress(host, port));
    }

    @Override
    public void sendMessage(UUID destination, PaxosMessage paxosMessage) {
        InetSocketAddress address = destinationMap.get(destination);
        if (address == null) {
            log.error("No address found for UUID: {}", destination);
            return;
        }

        log.debug("Env sending: {} -> {}", paxosMessage, destination);
        socketManagers.computeIfAbsent(destination, k -> new SocketManager(address)).sendMessage(paxosMessage);
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextMessage() {
        var result = messageQueue.take();
        log.debug("Env getting: {}", result);
        return result;
    }

    private void startServer() {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(serverPort);
                log.info("Listening for incoming Paxos messages on port {}", serverPort);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new MessageHandler(clientSocket)).start();
                }
            } catch (IOException e) {
                log.error("Server error: {}", e.getMessage());
            }
        }).start();
    }

    private class MessageHandler implements Runnable {
        private final Socket socket;

        public MessageHandler(Socket socket) {
            this.socket = socket;
        }

        @SneakyThrows
        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                while (true) {
                    try {
                        PaxosMessage message = (PaxosMessage) in.readObject();
                        messageQueue.put(message);
                    } catch (EOFException e) {
                        log.info("Client disconnected: {}", socket.getRemoteSocketAddress());
                        break;
                    } catch (ClassNotFoundException e) {
                        log.error("Invalid message format");
                    }
                }
            } catch (IOException | InterruptedException e) {
                log.error("Connection error: {}", e.getMessage());
            } finally {
                socket.close();
            }
        }
    }

    private static class SocketManager {
        private final InetSocketAddress address;
        private volatile Socket socket;
        private volatile ObjectOutputStream out;

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
                    out = new ObjectOutputStream(socket.getOutputStream());
                    log.info("Connected to {}", address);
                    return;
                } catch (IOException e) {
                    log.error("Connection attempt {} to {} failed: {}", attempt, address, e.getMessage());
                    Thread.sleep(1000);
                }
            }
        }

        public synchronized void sendMessage(PaxosMessage paxosMessage) {
            if (socket == null || socket.isClosed()) {
                connect();
            }
            if (socket != null && out != null) {
                try {
                    out.writeObject(paxosMessage);
                    out.flush();
                } catch (IOException e) {
                    log.error("Error sending message:", e);
                    connect();
                }
            }
        }

        @SneakyThrows
        private synchronized void close() {
            if (out != null) out.close();
            if (socket != null) socket.close();
        }
    }
}
