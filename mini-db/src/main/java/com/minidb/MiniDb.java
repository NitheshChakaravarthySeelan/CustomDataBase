
import com.minidb.index.BPlusTree;
import com.minidb.index.Serializer;
import com.minidb.log.RecoveryManager;
import com.minidb.log.WALManager;
import com.minidb.replication.RaftReplicator;
import com.minidb.raft.InMemoryRaftProtocol;
import com.minidb.serializers.IntegerSerializer;
import com.minidb.serializers.RecordIdSerializer;
import com.minidb.sql.executor.Executor;
import com.minidb.sql.executor.Result;
import com.minidb.sql.parser.Parser;
import com.minidb.sql.parser.Token;
import com.minidb.sql.parser.Tokenizer;
import com.minidb.storage.*;
import com.minidb.txn.LockManager;
import com.minidb.txn.TxnManager;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import com.minidb.DbComponents;
import com.minidb.MiniDbRepl;

public class MiniDb {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: MiniDb <nodeId> <peerId1,peerId2,...> <bPlusTreeOrder> <bufferPoolSize> <pageSize>");
            return;
        }
        String nodeId = args[0];
        List<String> peerIds = Arrays.asList(args[1].split(","));
        int bPlusTreeOrder = Integer.parseInt(args[2]);
        int bufferPoolSize = Integer.parseInt(args[3]);
        int pageSize = Integer.parseInt(args[4]);

        DbComponents components = initializeDbComponents(nodeId, peerIds, bPlusTreeOrder, bufferPoolSize, pageSize);

        try {
            startReplication(components.replicator);
            performRecovery(components.recoveryManager);

            MiniDbRepl repl = new MiniDbRepl(components.executor, nodeId);
            repl.start();
        } finally {
            cleanupDb(components);
        }
    }

    private static DbComponents initializeDbComponents(String nodeId, List<String> peerIds, int bPlusTreeOrder, int bufferPoolSize, int pageSize) throws IOException {
        File dbDir = new File("minidb_data_" + nodeId);
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }

        PageManager pageManager = new PageManager(new File(dbDir, "minidb.db").getPath(), pageSize);
        BufferPool bufferPool = new BufferPool(pageManager, bufferPoolSize);
        WALManager walManager = new WALManager(dbDir);
        Serializer<Integer> keySerializer = new IntegerSerializer();
        Serializer<RecordId> valueSerializer = new RecordIdSerializer();
        BPlusTree<Integer, RecordId> index = new BPlusTree<>(bPlusTreeOrder, keySerializer, valueSerializer, pageManager, bufferPool);
        LockManager lockManager = new LockManager();
        TxnManager txnManager = new TxnManager(lockManager, walManager);
        RecordsSerializer recordsSerializer = new RecordsSerializer(new RecordsSerializer.Column[]{
                new RecordsSerializer.Column("id", RecordsSerializer.ColumnType.INT),
                new RecordsSerializer.Column("value", RecordsSerializer.ColumnType.STRING)
        });
        RecordStorage recordStorage = new RecordStorage(bufferPool, recordsSerializer, walManager, index, pageManager);

        InMemoryRaftProtocol protocol = new InMemoryRaftProtocol();
        RaftReplicator replicator = new RaftReplicator(walManager, peerIds, nodeId, protocol);

        RecoveryManager recoveryManager = new RecoveryManager(walManager, recordStorage);

        Executor executor = new Executor(txnManager, walManager, lockManager, recordStorage);

        return new DbComponents(pageManager, bufferPool, walManager, index, lockManager, txnManager, recordsSerializer, recordStorage, replicator, recoveryManager, executor);
    }

    private static void startReplication(RaftReplicator replicator) throws IOException {
        replicator.start();
    }

    private static void performRecovery(RecoveryManager recoveryManager) throws IOException {
        recoveryManager.recover();
    }

    private static void cleanupDb(DbComponents components) throws Exception {
        System.out.println("\nFlushing pages and closing DB...");
        components.replicator.close();
        components.bufferPool.flushAllPages();
        components.walManager.close();
        components.pageManager.close();
        System.out.println("DB closed.");
    }
}