package ru.itmo.rusinov.consensus.kv.store.ratis;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import ru.itmo.rusinov.consensus.kv.store.ratis.server.KvStateMachine;
import ru.itmo.rusinov.consensus.kv.store.ratis.server.MapDbKvDatabase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KvStoreApplication {
    private static void run(String id, int port, List<RaftPeer> peers, String groupId) throws IOException, InterruptedException {
        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();

        GrpcConfigKeys.Server.setPort(properties, port);

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(Files.createTempDirectory("kvStorage").toFile()));
        StateMachine stateMachine = new KvStateMachine(new MapDbKvDatabase());

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(groupId)), peers);
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(peerId)
                .setStateMachine(stateMachine)
                .setProperties(properties).setGroup(raftGroup)
                .build();
        raftServer.start();

        while (raftServer.getLifeCycleState() != LifeCycle.State.CLOSED) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Starting raft kv store peer with peerId={}, port={}, peers={}", System.getenv("RATIS_KV_PEER_ID"), System.getenv("RATIS_KV_PORT"), System.getenv("RATIS_KV_PEERS"));

        String peerId = System.getenv("RATIS_KV_PEER_ID");
        int port = Integer.parseInt(System.getenv("RATIS_KV_PORT"));
        var peers = Arrays.stream(System.getenv("RATIS_KV_PEERS").split(","))
                .map((p) -> {
                    var addressParts = p.split(":");
                    return RaftPeer.newBuilder()
                            .setId(addressParts[0])
                            .setAddress(addressParts[1] + ":" + addressParts[2])
                            .build();
                })
                .toList();
        String groupId = System.getenv("RATIS_KV_GROUP_ID");

        run(peerId, port, peers, groupId);
    }
}
