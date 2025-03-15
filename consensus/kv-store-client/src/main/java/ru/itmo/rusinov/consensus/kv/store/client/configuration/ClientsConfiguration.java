package ru.itmo.rusinov.consensus.kv.store.client.configuration;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import ru.itmo.rusinov.consensus.common.NettyEnvironmentClient;
import ru.itmo.rusinov.consensus.common.SimpleEnvironmentClient;
import ru.itmo.rusinov.consensus.kv.store.client.paxos.PaxosClient;

import java.net.InetSocketAddress;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties(PeerConfigurationProperties.class)
public class ClientsConfiguration {

    @Bean
    public RaftClient raftClient(PeerConfigurationProperties peerConfigurationProperties) {
        var peers = peerConfigurationProperties.peers().stream().map((p) -> RaftPeer.newBuilder().setId(p.id()).setAddress(p.address()).build()).toList();
        var raftGroup = RaftGroupId.valueOf(ByteString.copyFromUtf8(peerConfigurationProperties.groupId()));

        var properties = new RaftProperties();

        return RaftClient.newBuilder().setRaftGroup(RaftGroup.valueOf(raftGroup, peers)).setProperties(properties).setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties)).setPrimaryDataStreamServer(peers.getFirst()).build();
    }

    @Bean
    public PaxosClient paxosClient(PeerConfigurationProperties peerConfigurationProperties) {
        var destinations = peerConfigurationProperties.peers()
                .stream()
                .collect(Collectors.toMap(
                        PeerConfigurationProperties.PeerConfiguration::id,
                        PeerConfigurationProperties.PeerConfiguration::address
                ));

        var envClient = new SimpleEnvironmentClient(destinations);
        envClient.initialize();

        return new PaxosClient(destinations.keySet().stream().toList(), envClient);
    }
}
