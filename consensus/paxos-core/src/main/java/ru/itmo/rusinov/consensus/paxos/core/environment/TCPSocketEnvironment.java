package ru.itmo.rusinov.consensus.paxos.core.environment;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.environment.tcp.SocketManager;
import ru.itmo.rusinov.consensus.paxos.core.message.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class TCPSocketEnvironment implements Environment {
    private final Map<UUID, InetSocketAddress> destinationMap = new ConcurrentHashMap<>();
    private final Map<UUID, SocketManager> socketManagers = new ConcurrentHashMap<>();

    private final BlockingQueue<PaxosMessage> acceptorQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosMessage> replicaQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosMessage> leaderQueue = new LinkedBlockingQueue<>();

    private final ConcurrentHashMap<UUID, LinkedBlockingQueue<PaxosMessage>> commanderQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, LinkedBlockingQueue<PaxosMessage>> scoutQueues = new ConcurrentHashMap<>();

    private final int serverPort;
    private ServerSocket serverSocket;

    public TCPSocketEnvironment(int serverPort) {
        this.serverPort = serverPort;
        startServer();
    }

    public TCPSocketEnvironment(int serverPort, Map<UUID, InetSocketAddress> destinationMap) {
        this.serverPort = serverPort;
        this.destinationMap.putAll(destinationMap);
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
    public PaxosMessage getNextAcceptorMessage() {
        return acceptorQueue.take();
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextLeaderMessage() {
        return leaderQueue.take();
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextScoutMessage(UUID scoutId) {
        scoutQueues.putIfAbsent(scoutId, new LinkedBlockingQueue<>());
        var queue = scoutQueues.get(scoutId);
        return queue.take();
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextCommanderMessage(UUID commanderId) {
        commanderQueues.putIfAbsent(commanderId, new LinkedBlockingQueue<>());
        var queue = commanderQueues.get(commanderId);
        return queue.take();
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextReplicaMessage() {
        return replicaQueue.take();
    }

    private void startServer() {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(serverPort);
                log.info("Listening for incoming Paxos messages on port {}", serverPort);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new MessageHandler(clientSocket), "TcpClientHandler").start();
                }
            } catch (IOException e) {
                log.error("Server error: {}", e.getMessage());
            }
        }, "TcpSocketEnvironment").start();
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
                        putMessageToQueues(message);
                    } catch (EOFException e) {
                        log.info("Client disconnected: {}", socket.getRemoteSocketAddress());
                        break;
                    } catch (ClassNotFoundException e) {
                        log.error("Invalid message format");
                    }
                }
            } catch (IOException e) {
                log.error("Connection error: {}", e.getMessage());
            } finally {
                socket.close();
            }
        }
    }

    @SneakyThrows
    private void putMessageToQueues(PaxosMessage paxosMessage) {
        switch (paxosMessage) {

            case RequestMessage rm -> replicaQueue.put(rm);
            case DecisionMessage dm -> replicaQueue.put(dm);

            case ProposeMessage pm -> leaderQueue.put(pm);
            case AdoptedMessage am -> leaderQueue.put(am);
            case PreemptedMessage pm -> leaderQueue.put(pm);

            case P1bMessage p1b -> scoutQueues
                    .computeIfAbsent(p1b.scoutId(), (uuid -> new LinkedBlockingQueue<>())).put(p1b);
            case P2bMessage p2b -> commanderQueues
                    .computeIfAbsent(p2b.commanderId(), (uuid -> new LinkedBlockingQueue<>())).put(p2b);

            case P1aMessage p1a -> acceptorQueue.put(p1a);
            case P2aMessage p2a -> acceptorQueue.put(p2a);

            default -> throw new IllegalStateException("Unexpected value: " + paxosMessage);
        }
    }
}
