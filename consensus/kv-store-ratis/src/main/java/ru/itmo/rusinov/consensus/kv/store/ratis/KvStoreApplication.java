package ru.itmo.rusinov.consensus.kv.store.ratis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Parameters(commandDescription = "Start an filestore server")
public class KvStoreApplication {

    @Parameter(names = {"--id", "-i"}, required = true)
    private String id;

    @Parameter(names = {"--peers", "-r"}, description = "id:host:port", required = true)
    private List<String> peers;

    @Parameter(names = {"--port", "-p"}, required = true)
    private int port;

    @Parameter(names = {"-g"})
    private String raftGroupId = "kvStoreRaftGroup";

    private void run() throws IOException, InterruptedException {
        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();

        GrpcConfigKeys.Server.setPort(properties, port);

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(Files.createTempDirectory("kvStorage").toFile()));
        StateMachine stateMachine = new KvStateMachine(new MapDbKvDatabase());

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(raftGroupId)), parsePeers());
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(peerId)
                .setStateMachine(stateMachine)
                .setProperties(properties).setGroup(raftGroup)
                .build();
        raftServer.start();

//        // todo remove client from here
//        KvStoreRatisClient client = new KvStoreRatisClient(raftGroup, properties);
//        client.test();

        for (; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED; ) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        KvStoreApplication main = new KvStoreApplication();
        JCommander.newBuilder().addObject(main).build().parse(args);
        main.run();
    }

    private RaftPeer[] parsePeers() {
        return peers.stream().map((p) -> {
            var addressParts = p.split(":");
            return RaftPeer.newBuilder().setId(addressParts[0]).setAddress(addressParts[1] + ":" + addressParts[2]).build();
        }).toArray(RaftPeer[]::new);
    }
}
