package ru.itmo.rusinov.consensus.kv.store;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.impl.JvmMetrics;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import ru.itmo.rusinov.consensus.kv.store.paxos.KvStorePaxosStateMachine;
import ru.itmo.rusinov.consensus.kv.store.ratis.KvStateMachine;
import ru.itmo.rusinov.consensus.kv.store.db.MapDbKvDatabase;
import ru.itmo.rusinov.consensus.paxos.core.PaxosServer;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class KvStoreApplication {

    @SneakyThrows
    private static File createStorageDir(String path) {
        if (Objects.isNull(path)) {
            return Files.createTempDirectory("kvStorage").toFile();
        } else {
            var file = new File(path);
            file.mkdirs();
            return file;
        }
    }

    @SneakyThrows
    private static void runOnRatis() {
        String id = System.getenv("RATIS_KV_PEER_ID");
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
        String storagePath = System.getenv("RATIS_KV_STORAGE_PATH");

        final MetricRegistries registries = MetricRegistries.global();
        JvmMetrics.addJvmMetrics(registries);
        registries.enableJmxReporter();

        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();

        GrpcConfigKeys.Server.setPort(properties, port);

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(createStorageDir(storagePath)));
        StateMachine stateMachine = new KvStateMachine(new MapDbKvDatabase());

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(groupId)), peers);
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(peerId)
                .setStateMachine(stateMachine)
                .setOption(RaftStorage.StartupOption.RECOVER)
                .setProperties(properties).setGroup(raftGroup)
                .build();
        raftServer.start();

        while (raftServer.getLifeCycleState() != LifeCycle.State.CLOSED) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @SneakyThrows
    private static void runOnPaxos() {
        String id = System.getenv("RATIS_KV_PEER_ID");
        int port = Integer.parseInt(System.getenv("RATIS_KV_PORT"));
        var destinations = Arrays.stream(System.getenv("RATIS_KV_PEERS").split(","))
                .collect(Collectors.toMap(
                        (p) -> p.split(":")[0],
                        (p) -> {
                            var addressParts = p.split(":");
                            return new InetSocketAddress(addressParts[1], Integer.parseInt(addressParts[2]));
                        }
                ));
        var config = new Config(destinations.keySet());
        String storagePath = System.getenv("RATIS_KV_STORAGE_PATH");
        var stateMachine = new KvStorePaxosStateMachine(new MapDbKvDatabase());

        var server = new PaxosServer(
                id, stateMachine, new TCPSocketEnvironment(port, destinations), config, new File(storagePath)
        );

        server.start();

        while (true) {
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) {
        String protocol = System.getenv("KV_STORE_PROTOCOL");

        if (protocol.equals("paxos")) {
            runOnPaxos();
        } else {
            runOnRatis();
        }
    }
}
