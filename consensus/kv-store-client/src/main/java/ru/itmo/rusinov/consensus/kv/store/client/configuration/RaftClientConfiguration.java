package ru.itmo.rusinov.consensus.kv.store.client.configuration;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RaftConfigurationProperties.class)
public class RaftClientConfiguration {

    @Bean
    public RaftClient raftClient(RaftConfigurationProperties raftConfigurationProperties) {
        var peers = raftConfigurationProperties.peers()
                .stream()
                .map((p) -> RaftPeer.newBuilder().setId(p.id()).setAddress(p.address()).build())
                .toList();
        var raftGroup = RaftGroupId.valueOf(ByteString.copyFromUtf8(raftConfigurationProperties.groupId()));

        var properties = new RaftProperties();

        return RaftClient.newBuilder()
                .setRaftGroup(RaftGroup.valueOf(raftGroup, peers))
                .setProperties(properties)
                .setClientRpc(
                        new GrpcFactory(new Parameters())
                                .newRaftClientRpc(ClientId.randomId(), properties)
                )
                .setPrimaryDataStreamServer(peers.getFirst())
                .build();
    }
}
