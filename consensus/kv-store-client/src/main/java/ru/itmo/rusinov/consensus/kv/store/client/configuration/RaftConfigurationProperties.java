package ru.itmo.rusinov.consensus.kv.store.client.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("raft")
public record RaftConfigurationProperties(
        String groupId,
        List<RaftPeerConfiguration> peers
) {

    public record RaftPeerConfiguration(
            String id,
            String address
    ) {
    }
}
