package ru.itmo.rusinov.consensus.raft.core;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Objects;
import java.util.Random;

@Slf4j
public class TimeoutTimer {
    private final Random random = new Random();
    private volatile Instant nextTimeout;

    public TimeoutTimer() {
        resetTimer();
    }

    public void resetTimer() {
        nextTimeout = Instant.now().plusMillis(random.nextInt(1000, 3000));
    }

    public void deactivateTimer() {
        nextTimeout = null;
    }

    public boolean isTimeout() {
        return Objects.nonNull(nextTimeout) && nextTimeout.isBefore(Instant.now());
    }
}
