package ru.itmo.rusinov.consensus.raft.core;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import raft.Raft;
import ru.itmo.rusinov.consensus.common.DistributedServer;
import ru.itmo.rusinov.consensus.common.EnvironmentClient;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class RaftServer {
    private final DurableStateStore durableStateStore;
    private final String id;

    private long currentTerm;
    private String votedFor;
    private final Map<Long, Raft.LogEntry> logEntries;
    private long lastIndex = 0;
    private long commitIndex = 0;
    private long lastApplied = 0;

    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private final Set<String> replicas;

    private final EnvironmentClient environmentClient;
    private final DistributedServer distributedServer;

    private final StateMachine stateMachine;


    private final Set<String> currentElectionVoters = new HashSet<>();

    private final BlockingQueue<Raft.RaftMessage> messagesQueue = new LinkedBlockingQueue<>();

    private volatile RaftRole currentRole;
    private final TimeoutTimer electionTimer = new TimeoutTimer();

    private final Map<UUID, CompletableFuture<Raft.ClientResponse>> awaitingClientRequests = new HashMap<>();
    private final Map<Long, CompletableFuture<Raft.ClientResponse>> inFlightClientRequests = new HashMap<>();

    private Map<String, Long> maxSentLogs = new HashMap<>();
    private final File storagePath;

    public RaftServer(DurableStateStore durableStateStore, String id, Set<String> replicas, EnvironmentClient environmentClient, DistributedServer distributedServer, StateMachine stateMachine, File storagePath) {
        durableStateStore.initialize(storagePath);
        stateMachine.initialize(storagePath);

        this.durableStateStore = durableStateStore;
        currentTerm = durableStateStore.loadCurrentTerm().orElse(0L);
        votedFor = durableStateStore.loadVotedFor().orElse(null);
        logEntries = durableStateStore.loadLog();
        this.id = id;
        this.replicas = Set.copyOf(replicas);
        this.environmentClient = environmentClient;
        this.distributedServer = distributedServer;
        this.stateMachine = stateMachine;
        this.storagePath = storagePath;

        lastIndex = logEntries.keySet().stream().max(Comparator.naturalOrder()).orElse(0L);
        currentRole = RaftRole.FOLLOWER;
    }

    public void start() {
        distributedServer.setRequestHandler(this::handleMessage);
        distributedServer.initialize();
        environmentClient.initialize();

        new Thread(new ElectionTimerChecker()).start();
        new Thread(new HeartbeatSender(Duration.ofMillis(25))).start();
        new Thread(new ServerThread()).start();
    }

    private void handleMessage(Raft.RaftMessage raftMessage) {
        updateTerm(raftMessage);

        if (!raftMessage.getMessageCase().equals(Raft.RaftMessage.MessageCase.HEARTBEAT)) {
            log.info("{} handling {} from {}", id, raftMessage.getMessageCase(), raftMessage.getSrc());
        }

        switch (raftMessage.getMessageCase()) {
            case REQUEST_VOTE -> handleRequestVote(raftMessage);
            case APPEND_ENTRIES -> handleAppendEntries(raftMessage);
            case ELECTION_TIMEOUT_ELAPSED -> convertToCandidate();
            case REQUEST_VOTE_RESULT -> handleRequestVoteResult(raftMessage);
            case CLIENT_REQUEST -> handleClientRequest(raftMessage);
            case APPEND_ENTRIES_RESULT -> handleAppendEntriesResult(raftMessage);
            case HEARTBEAT -> handleHeartBeat(raftMessage);
        }
    }

    private void handleHeartBeat(Raft.RaftMessage raftMessage) {
        electionTimer.resetTimer();
        if (raftMessage.getTerm() >= currentTerm) {
            votedFor = raftMessage.getSrc();
            if (!currentRole.equals(RaftRole.FOLLOWER)) {
                convertToFollower();
            }
        }
    }

    private void handleAppendEntriesResult(Raft.RaftMessage raftMessage) {
        if (!currentRole.equals(RaftRole.LEADER)) {
            return;
        }

        var appendEntriesResult = raftMessage.getAppendEntriesResult();
        if (!appendEntriesResult.getSuccess()) {
            nextIndex.compute(raftMessage.getSrc(), (k, prevNextIndex) -> prevNextIndex - 1);
        } else {
            matchIndex.put(raftMessage.getSrc(), nextIndex.get(raftMessage.getSrc()));
            nextIndex.compute(raftMessage.getSrc(), (k, prevNextIndex) -> prevNextIndex + 1);
        }
        maxSentLogs.remove(raftMessage.getSrc());
    }

    private void handleClientRequest(Raft.RaftMessage raftMessage) {
        var clientRequest = raftMessage.getClientRequest();
        var requestId = UUID.fromString(clientRequest.getRequestId());
        var future = awaitingClientRequests.remove(requestId);
        var responseBuilder = Raft.ClientResponse.newBuilder();

        if (!currentRole.equals(RaftRole.LEADER)) {
            responseBuilder.setSuggestedLeader(votedFor);
            future.complete(responseBuilder.build());
            log.info("Not a leader, skipping client request");
            return;
        }

        var logEntry = Raft.LogEntry.newBuilder()
                .setTerm(currentTerm)
                .setCommand(clientRequest.getClientRequest().getRequest())
                .build();
        var selectedIndex = lastIndex + 1;
        lastIndex = lastIndex + 1;

        inFlightClientRequests.put(selectedIndex, future);
        logEntries.put(selectedIndex, logEntry);
        durableStateStore.addLog(selectedIndex, logEntry);
    }

    private void handleRequestVoteResult(Raft.RaftMessage raftMessage) {
        if (!currentRole.equals(RaftRole.CANDIDATE)) {
            return;
        }

        var requestVoteResult = raftMessage.getRequestVoteResult();

        if (requestVoteResult.getVoteGranted()) {
            currentElectionVoters.add(raftMessage.getSrc());
        }

        if (currentElectionVoters.size() >= quorum()) {
            convertToLeader();
        }
    }

    private void handleAppendEntries(Raft.RaftMessage raftMessage) {
        if (!currentRole.equals(RaftRole.FOLLOWER)) {
            return;
        }

        var appendEntries = raftMessage.getAppendEntries();
        var responseBuilder = Raft.RaftMessage.newBuilder()
                .setSrc(id)
                .setTerm(currentTerm);

        if (raftMessage.getTerm() < currentTerm) {
            log.info("Ignoring append entries because term is too old: {} < {}", raftMessage.getTerm(), currentTerm);
            responseBuilder.getAppendEntriesResultBuilder().setSuccess(false);
            sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
            return;
        }

        var prevLogCheck = appendEntries.hasPrevLogIndex() && appendEntries.hasPrevLogTerm();

        if (prevLogCheck) {
            var prevLog = logEntries.get(appendEntries.getPrevLogIndex());
            if (Objects.isNull(prevLog) || prevLog.getTerm() != appendEntries.getPrevLogTerm()) {
                log.info("Ignoring append entries because prev log by index {} term {} not found", appendEntries.getPrevLogIndex(), appendEntries.getPrevLogTerm());
                responseBuilder.getAppendEntriesResultBuilder().setSuccess(false);
                sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
                return;
            }
        }

        var entriesIndex = appendEntries.getPrevLogIndex() + 1;
        for (var e : appendEntries.getEntriesList()) {
            logEntries.put(entriesIndex, e);
            durableStateStore.addLog(entriesIndex, e);
        }
        lastIndex = entriesIndex;

        if (appendEntries.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(appendEntries.getLeaderCommit(), entriesIndex);
        }

        responseBuilder.getAppendEntriesResultBuilder().setSuccess(true);
        sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
    }

    private void handleRequestVote(Raft.RaftMessage raftMessage) {
        if (!currentRole.equals(RaftRole.FOLLOWER)) {
            return;
        }

        var requestVote = raftMessage.getRequestVote();
        var responseBuilder = Raft.RaftMessage.newBuilder()
                .setTerm(currentTerm)
                .setSrc(id);

        if (raftMessage.getTerm() < currentTerm) {
            responseBuilder.getRequestVoteResultBuilder().setVoteGranted(false);
            sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
            return;
        }

        if (Objects.nonNull(votedFor) && !votedFor.equals(requestVote.getCandidateId())) {
            log.info("{} already voted for {}", id, votedFor);
            responseBuilder.getRequestVoteResultBuilder().setVoteGranted(false);
            sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
            return;
        }

        var lastTermIndex = lastIndex == 0 ? null : new TermIndex(logEntries.get(lastIndex).getTerm(), lastIndex);
        if (Objects.nonNull(lastTermIndex)
                && lastTermIndex.compareTo(new TermIndex(requestVote.getLastLogTerm(), requestVote.getLastLogIndex())) > 0) {
            responseBuilder.getRequestVoteResultBuilder().setVoteGranted(false);
            sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
            return;
        }
        electionTimer.resetTimer();

        votedFor = raftMessage.getSrc();
        durableStateStore.saveVotedFor(raftMessage.getSrc());
        currentRole = RaftRole.FOLLOWER;

        responseBuilder.getRequestVoteResultBuilder().setVoteGranted(true);
        sendRequestToOtherServer(responseBuilder.build(), raftMessage.getSrc());
    }

    private void applyLogs() {
        while (commitIndex > lastApplied) {
            log.info("Applying logs: commitIndex={}, lastApplied={}, requests: {}", commitIndex, lastApplied, inFlightClientRequests);
            lastApplied++;
            var result = stateMachine.applyCommand(logEntries.get(lastApplied).getCommand().getValue().toByteArray());

            if (!currentRole.equals(RaftRole.LEADER)) {
                continue;
            }
            var request = inFlightClientRequests.remove(lastApplied);

            if (Objects.isNull(request)) {
                continue;
            }

            var resultResponse = Raft.ClientResponse
                    .newBuilder()
                    .setCommandResult(Raft.RaftCommandResult
                            .newBuilder()
                            .setValue(ByteString.copyFrom(result)).build())
                    .build();
            request.complete(resultResponse);
        }
    }

    private void updateTerm(Raft.RaftMessage raftMessage) {
        if (currentTerm < raftMessage.getTerm()) {
            currentTerm = raftMessage.getTerm();
            durableStateStore.saveCurrentTerm(currentTerm);
            convertToFollower();
        }
    }

    private void convertToLeader() {
        log.info("{} -> {}", id, RaftRole.LEADER);
        sendHeartBeats();

        var maxLogIndex = logEntries.keySet()
                .stream()
                .max(Comparator.naturalOrder());

        matchIndex.clear();
        nextIndex.clear();
        maxSentLogs.clear();

        for (var r : replicas) {
            if (!r.equals(id)) {
                nextIndex.put(r, maxLogIndex.orElse(1L));
                matchIndex.put(r, 0L);
            }
        }

        currentRole = RaftRole.LEADER;
    }

    private void convertToFollower() {
        log.info("{} -> {}", id, RaftRole.FOLLOWER);
        electionTimer.resetTimer();
        votedFor = null;

        currentRole = RaftRole.FOLLOWER;
    }

    private void convertToCandidate() {
        log.info("{} -> {}", id, RaftRole.CANDIDATE);
        electionTimer.resetTimer();

        currentRole = RaftRole.CANDIDATE;

        currentTerm++;
        durableStateStore.saveCurrentTerm(currentTerm);
        currentElectionVoters.clear();

        votedFor = id;
        currentElectionVoters.add(id);


        var requestVoteMessageBuilder = Raft.RaftMessage.newBuilder();
        requestVoteMessageBuilder.setSrc(id);
        requestVoteMessageBuilder.setTerm(currentTerm);

        requestVoteMessageBuilder.getRequestVoteBuilder().setCandidateId(id);

        if (lastIndex != 0) {
            requestVoteMessageBuilder.getRequestVoteBuilder().setLastLogIndex(lastIndex);
            requestVoteMessageBuilder.getRequestVoteBuilder().setLastLogTerm(logEntries.get(lastIndex).getTerm());
        }

        var message = requestVoteMessageBuilder.build();

        replicas.stream()
                .filter((r) -> !r.equals(id))
                .forEach((r) -> sendRequestToOtherServer(message, r));
    }

    @SneakyThrows
    private CompletableFuture<byte[]> handleMessage(byte[] message) {
        var parsedMessage = Raft.RaftServerRequest.parseFrom(message);

        if (parsedMessage.getMessageCase().equals(Raft.RaftServerRequest.MessageCase.CLIENT_REQUEST)) {
            var requestId = UUID.randomUUID();

            messagesQueue.put(
                    Raft.RaftMessage.newBuilder()
                            .setClientRequest(
                                    Raft.InternalClientRequest.newBuilder()
                                            .setClientRequest(parsedMessage.getClientRequest())
                                            .setRequestId(requestId.toString())
                                            .build()
                            )
                            .build()
            );
            var future = new CompletableFuture<Raft.ClientResponse>();
            awaitingClientRequests.put(requestId, future);

            return future.thenApply(AbstractMessageLite::toByteArray);
        } else {
            messagesQueue.put(parsedMessage.getRaftMessage());

            return CompletableFuture.completedFuture(new byte[0]);
        }
    }

    private void updateFollowers() {
        if (!currentRole.equals(RaftRole.LEADER)) {
            return;
        }

        for (var r : replicas) {
            if (r.equals(id) || maxSentLogs.getOrDefault(r, 0L) >= nextIndex.get(r)) {
                continue;
            }
            // fixme do not commit from prev terms
            if (nextIndex.get(r) > lastIndex) {
                continue;
            }

            var message = Raft.RaftMessage.newBuilder();
            message.setSrc(id);
            message.setTerm(currentTerm);

            var prevLogIndex = nextIndex.get(r) - 1;
            var prevLog = logEntries.get(prevLogIndex);
            if (Objects.nonNull(prevLog)) {
                var prevLogTerm = logEntries.get(prevLogIndex).getTerm();
                message.getAppendEntriesBuilder().setPrevLogIndex(prevLogIndex);
                message.getAppendEntriesBuilder().setPrevLogTerm(prevLogTerm);
            }

            message.getAppendEntriesBuilder().setLeaderId(id);
            message.getAppendEntriesBuilder().addEntries(logEntries.get(nextIndex.get(r)));
            message.getAppendEntriesBuilder().setLeaderCommit(commitIndex);

            maxSentLogs.put(r, nextIndex.get(r));

            sendRequestToOtherServer(message.build(), r);
        }
    }

    private class ServerThread implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    handleMessage(messagesQueue.take());
                    updateFollowers();
                    updateCommitIndex();
                    applyLogs();
                }
            } catch (Exception e) {
                log.error("Message poller failed", e);
                return;
            }
        }
    }


    private void updateCommitIndex() {
        if (!currentRole.equals(RaftRole.LEADER)) {
            return;
        }

        var minCommit = commitIndex;
        for (var v : matchIndex.values()) {
            if (v > minCommit && matchIndex.values().stream().filter((it) -> it >= v).count() >= (replicas.size() / 2)) {
                minCommit = v;
            }
        }

        log.info("Updating min commit with {}", minCommit);
        if (minCommit == 0 || logEntries.get(minCommit).getTerm() != currentTerm) {
            return;
        }

        commitIndex = Math.max(commitIndex, minCommit);
    }

    private class ElectionTimerChecker implements Runnable {
        private final Duration tickDuration = Duration.ofMillis(250);

        @Override
        public void run() {
            while (true) {
                try {
                    if (currentRole.equals(RaftRole.LEADER)) {
                        continue;
                    }

                    if (electionTimer.isTimeout()) {
                        var message = Raft.RaftMessage
                                .newBuilder()
                                .setElectionTimeoutElapsed(Raft.ElectionTimeoutElapsed.newBuilder())
                                .build();

                        messagesQueue.put(message);
                        electionTimer.deactivateTimer();
                    }

                    Thread.sleep(tickDuration);
                } catch (Exception e) {
                    log.error("Timer failed", e);
                    return;
                }
            }
        }
    }

    private class HeartbeatSender implements Runnable {
        private final Duration frequency;

        private HeartbeatSender(Duration frequency) {
            this.frequency = frequency;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (!currentRole.equals(RaftRole.LEADER)) {
                        continue;
                    }

                    sendHeartBeats();

                    Thread.sleep(frequency);
                } catch (Exception e) {
                    log.error("Heartbeat sender failed", e);
                    return;
                }
            }
        }
    }

    void sendHeartBeats() {
        var message = Raft.RaftMessage.newBuilder()
                .setHeartbeat(Raft.HeartBeat.newBuilder())
                .setSrc(id)
                .setTerm(currentTerm)
                .build();
        replicas.stream()
                .filter((r) -> !r.equals(id))
                .forEach((r) -> sendRequestToOtherServer(message, r));
    }

    private void sendRequestToOtherServer(Raft.RaftMessage raftMessage, String serverId) {
        var serverRequest = Raft.RaftServerRequest.newBuilder()
                .setRaftMessage(raftMessage)
                .build();
        environmentClient.sendMessage(serverRequest.toByteArray(), serverId);
    }

    private int quorum() {
        return replicas.size() / 2 + 1;
    }
}
