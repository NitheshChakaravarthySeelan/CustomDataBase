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

public class SlaveRecoveryTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testSlaveRecovery() throws Exception {
        // 1. Setup master and add some records
        File masterWalDir = tempFolder.newFolder("master_recovery");
        WALManager masterWal = new WALManager(masterWalDir);
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key1".getBytes(), "val1".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key2".getBytes(), "val2".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key3".getBytes(), "val3".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key4".getBytes(), "val4".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key5".getBytes(), "val5".getBytes()));

        int port = findFreePort();
        try (Replicator replicator = new Replicator(masterWal, port)) {
            replicator.start();

            // 2. Setup slave and let it receive some records
            File slaveWalDir = tempFolder.newFolder("slave_recovery");
            WALManager slaveWal1 = new WALManager(slaveWalDir);
            SlaveNode slave1 = new SlaveNode("localhost", port, slaveWal1);
            CountDownLatch latch1 = new CountDownLatch(3);
            slave1.setRecordsReceivedLatch(latch1);
            slave1.start();

            // Let slave receive first 3 records
            assertTrue("Slave 1 did not receive 3 records in time", latch1.await(5, TimeUnit.SECONDS));
            assertEquals(3, slave1.getReceivedRecords().size());
            assertEquals(3, slave1.getLastAppliedLSN());

            // 3. Simulate crash: close slave1
            slave1.close();
            slaveWal1.close();

            // 4. Restart slave: create new SlaveNode with same WAL directory
            WALManager slaveWal2 = new WALManager(slaveWalDir);
            SlaveNode slave2 = new SlaveNode("localhost", port, slaveWal2);
            CountDownLatch latch2 = new CountDownLatch(2); // Remaining 2 records
            slave2.setRecordsReceivedLatch(latch2);
            slave2.start();

            // Verify it loaded last applied LSN
            assertEquals(3, slave2.getLastAppliedLSN());

            // 5. Let slave2 catch up
            assertTrue("Slave 2 did not receive remaining 2 records in time", latch2.await(5, TimeUnit.SECONDS));

            // 6. Assert slave2 received all records
            assertEquals(2, slave2.getReceivedRecords().size()); // Only 2 records received in this run
            assertEquals(5, slave2.getLastAppliedLSN());

            slave2.close();
            slaveWal2.close();
        }
        masterWal.close();
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
