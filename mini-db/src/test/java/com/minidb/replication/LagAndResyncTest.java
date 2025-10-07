package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LagAndResyncTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testReplicationLag() throws Exception {
        // 1. Setup master
        File masterWalDir = tempFolder.newFolder("master_lag");
        WALManager masterWal = new WALManager(masterWalDir);
        int port = findFreePort();
        try (Replicator replicator = new Replicator(masterWal, port)) {
            replicator.start();

            // 2. Setup slave with a significant ACK delay
            File slaveWalDir = tempFolder.newFolder("slave_lag");
            WALManager slaveWal = new WALManager(slaveWalDir);
            SlaveNode slave = new SlaveNode("localhost", port, slaveWal);
            slave.setAckDelayMillis(200); // Introduce a 200ms delay for ACKs
            slave.start();

            // Give time for slave to connect
            Thread.sleep(200);

            // 3. Append records to master
            masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key1".getBytes(), "val1".getBytes()));
            masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key2".getBytes(), "val2".getBytes()));
            masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key3".getBytes(), "val3".getBytes()));

            // 4. Wait for master to send records, but slave will be slow to ACK
            Thread.sleep(500);

            // 5. Assert master detects lag
            String slaveId = String.valueOf(slave.hashCode());
            ReplicationState slaveState = replicator.getSlaves().get(slaveId);
            assertNotNull(slaveState);
            assertTrue("Slave should be lagging", slaveState.getLastSentLSN() > slaveState.getLastAckLSN());

            // 6. Remove delay and let slave catch up
            slave.setAckDelayMillis(0);
            Thread.sleep(500); // Give time for ACKs to be processed

            // 7. Assert slave caught up
            assertEquals(slaveState.getLastSentLSN(), slaveState.getLastAckLSN());
            assertEquals(3, slave.getReceivedRecords().size());

            slave.close();
            slaveWal.close();
        }
        masterWal.close();
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
