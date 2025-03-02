package ru.itmo.rusinov.consensus.kv.store.client.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("servers")
public record PeerConfigurationProperties(
        String groupId,
        List<PeerConfiguration> peers
) {

    public record PeerConfiguration(
            String id,
            String address
    ) {
    }
}
