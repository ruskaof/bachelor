package ru.itmo.rusinov.consensus.paxos.core.environment;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.PaxosMessage;
import ru.itmo.rusinov.consensus.paxos.core.environment.tcp.SocketManager;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class TCPSocketEnvironment implements Environment {
    private final Map<String, InetSocketAddress> destinationMap = new ConcurrentHashMap<>();
    private final Map<String, SocketManager> socketManagers = new ConcurrentHashMap<>();
    private final Map<String, OutputStream> clientOutputs = new ConcurrentHashMap<>();

    private final BlockingQueue<PaxosMessage> acceptorQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosMessage> replicaQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PaxosMessage> leaderQueue = new LinkedBlockingQueue<>();

    private final ConcurrentHashMap<String, LinkedBlockingQueue<PaxosMessage>> commanderQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LinkedBlockingQueue<PaxosMessage>> scoutQueues = new ConcurrentHashMap<>();

    private final int serverPort;
    private ServerSocket serverSocket;

    public TCPSocketEnvironment(int serverPort) {
        this.serverPort = serverPort;
        startServer();
    }

    public TCPSocketEnvironment(int serverPort, Map<String, InetSocketAddress> destinationMap) {
        this.serverPort = serverPort;
        this.destinationMap.putAll(destinationMap);
        startServer();
    }

    public void addDestination(String uuid, String host, int port) {
        destinationMap.put(uuid, new InetSocketAddress(host, port));
    }

    @Override
    public void sendMessage(String destination, PaxosMessage paxosMessage) {
        InetSocketAddress address = destinationMap.get(destination);
        if (address == null) {
            log.error("No address found for UUID: {}", destination);
            return;
        }

        log.debug("Env sending: {} -> {}", paxosMessage, destination);
        socketManagers.computeIfAbsent(destination, k -> new SocketManager(address)).sendMessage(paxosMessage);
    }

    @Override
    public void sendResponse(String clientId, Paxos.CommandResult response) {
        try {
            OutputStream outputStream = clientOutputs.get(clientId);
            if (outputStream != null) {
                response.writeDelimitedTo(outputStream);
                log.debug("Sent response to client: {}", clientId);
            } else {
                log.warn("No output stream found for client: {}", clientId);
            }
        } catch (IOException e) {
            log.error("Failed to send response to client: {}", clientId, e);
        }
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
        scoutQueues.putIfAbsent(scoutId.toString(), new LinkedBlockingQueue<>());
        return scoutQueues.get(scoutId.toString()).take();
    }

    @SneakyThrows
    @Override
    public PaxosMessage getNextCommanderMessage(UUID commanderId) {
        commanderQueues.putIfAbsent(commanderId.toString(), new LinkedBlockingQueue<>());
        return commanderQueues.get(commanderId.toString()).take();
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
        private final OutputStream outputStream;
        private String clientId;

        public MessageHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.outputStream = socket.getOutputStream();
        }

        @SneakyThrows
        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        PaxosMessage message = PaxosMessage.parseDelimitedFrom(socket.getInputStream());

                        if (message.getMessageCase() == PaxosMessage.MessageCase.REQUEST) {
                            clientId = message.getRequest().getCommand().getClientId();
                            clientOutputs.put(message.getRequest().getCommand().getClientId(), outputStream);
                        }

                        putMessageToQueues(message);
                    } catch (EOFException e) {
                        log.info("Client disconnected: {}", socket.getRemoteSocketAddress());
                        break;
                    }
                }
            } catch (IOException e) {
                log.error("Connection error: {}", e);
            } finally {
                clientOutputs.remove(clientId);
                socket.close();
            }
        }
    }

    @SneakyThrows
    private void putMessageToQueues(PaxosMessage paxosMessage) {
        switch (paxosMessage.getMessageCase()) {

            case REQUEST, DECISION -> replicaQueue.put(paxosMessage);

            case PROPOSE, ADOPTED, PREEMPTED -> leaderQueue.put(paxosMessage);

            case P1B -> scoutQueues
                    .computeIfAbsent(paxosMessage.getP1B().getScoutId(), (uuid -> new LinkedBlockingQueue<>())).put(paxosMessage);
            case P2B -> commanderQueues
                    .computeIfAbsent(paxosMessage.getP2B().getCommanderId(), (uuid -> new LinkedBlockingQueue<>())).put(paxosMessage);

            case P1A, P2A -> acceptorQueue.put(paxosMessage);

            default -> throw new IllegalStateException("Unexpected value: " + paxosMessage);
        }
    }
}
