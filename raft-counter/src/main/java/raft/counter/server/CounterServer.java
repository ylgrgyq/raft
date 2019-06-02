package raft.counter.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CounterServer {

    public static void main(String[] args) {

        SpringApplication.run(CounterServer.class, args);

//        String persistentStateDir = Paths.get(TestingConfigs.persistentStateDir, testingName).toString();
//        String storageDir = Paths.get(TestingConfigs.testingStorageDirectory, testingName).toString();
//        String storageName = testingName + "_" + "LogStorage";
//        String configBuilder = RaftConfigurations.newBuilder()
//                .withPersistentMetaFileDirPath(this.persistentStateDir);
//
//        FileBasedStorage storage = new FileBasedStorage(storageDir, getStorageName(peerId));
//        StateMachine stateMachine = new CounterStateMachine();
//
//        RaftConfigurations c = configBuilder
//                .withPeers(peers)
//                .withSelfID(peerId)
////                .withRaftCommandBroker(new TestingBroker(nodes, logger))
//                .withStateMachine(stateMachine)
//                .withPersistentMetaFileDirPath(persistentStateDir)
//                .withPersistentStorage(storage)
//                .build();
//        Raft raft = new RaftImpl(c);
    }
}
