package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;
import com.minidb.replication.transport.Message;
import com.minidb.replication.transport.SocketChannel;
import com.minidb.replication.transport.TcpSocketChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ReplicationFlowTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testHandshake() throws Exception {
        int port = findFreePort();
        CountDownLatch latch = new CountDownLatch(1);

        Thread serverThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                Socket clientSocket = serverSocket.accept();
                SocketChannel serverChannel = new TcpSocketChannel(clientSocket);

                serverChannel.setOnMessage(message -> {
                    assertEquals(SyncProtocol.MessageType.HELLO, message.getMsgType());
                    try {
                        Message pong = Message.pong(System.currentTimeMillis(), message.getCorrelationId());
                        serverChannel.send(pong);
                    } catch (IOException e) {
                        fail(e.getMessage());
                    }
                });
                serverChannel.startListening();
            } catch (IOException e) {
                // ignore
            }
        });

        serverThread.start();

        // Give server time to start
        Thread.sleep(100);

        SocketChannel clientChannel = new TcpSocketChannel("localhost", port);
        clientChannel.connect("localhost", port);
        clientChannel.setOnMessage(message -> {
            assertEquals(SyncProtocol.MessageType.PONG, message.getMsgType());
            latch.countDown();
        });
        clientChannel.startListening();

        Message hello = Message.hello(123L, 456L, "{}", 789L);
        clientChannel.send(hello);

        assertTrue("Test timed out", latch.await(5, TimeUnit.SECONDS));

        clientChannel.close();
        serverThread.join();
    }

    @Test
    public void testReplicatorListensToWAL() throws Exception {
        File walDir = tempFolder.newFolder();
        WALManager walManager = new WALManager(walDir);
        int port = findFreePort();
        try (Replicator replicator = new Replicator(walManager, port)) {
            replicator.start();

            LogRecord record = new LogRecord(0L, LogRecord.OP_PUT, 1L, "key1".getBytes(), "value1".getBytes());
            long lsn = walManager.appendAndFlush(record);

            // Allow some time for async notification
            Thread.sleep(100);

            assertNotNull(replicator.getLastReceivedRecord());
            assertEquals(lsn, replicator.getLastReceivedRecord().getLsn());

            walManager.close();
        }
    }

    @Test
    public void testEndToEndReplication() throws Exception {
        // 1. Setup master
        File masterWalDir = tempFolder.newFolder("master");
        WALManager masterWal = new WALManager(masterWalDir);
        int port = findFreePort();
        try (Replicator replicator = new Replicator(masterWal, port)) {
            replicator.start();

            // 2. Setup slave
            File slaveWalDir = tempFolder.newFolder("slave");
            WALManager slaveWal = new WALManager(slaveWalDir);
            try (SlaveNode slave = new SlaveNode("localhost", port, slaveWal)) {
                slave.start();

                // Give time for slave to connect
                Thread.sleep(200);

                // 3. Append a record to master
                LogRecord record = new LogRecord(0L, LogRecord.OP_PUT, 1L, "testKey".getBytes(), "testValue".getBytes());
                long lsn = masterWal.appendAndFlush(record);

                // 4. Wait for replication
                Thread.sleep(200);

                // 5. Assert slave received it
                LogRecord replicated = slave.getLastReceivedRecord();
                assertNotNull(replicated);
                assertEquals(lsn, replicated.getLsn());
                assertEquals(record.getType(), replicated.getType());
                assertTrue(Arrays.equals(record.getKey(), replicated.getKey()));
                assertTrue(Arrays.equals(record.getValue(), replicated.getValue()));

                // 6. Assert master received ACK
                Thread.sleep(200); // wait for ack to be processed
                String slaveId = String.valueOf(slave.hashCode());
                ReplicationState slaveState = replicator.getSlaves().get(slaveId);
                assertNotNull(slaveState);
                assertEquals(lsn, slaveState.getLastAckLSN());
            }
            slaveWal.close();
        }
        masterWal.close();
    }

    @Test
    public void testSlaveCatchUp() throws Exception {
        // 1. Setup master and add some records
        File masterWalDir = tempFolder.newFolder("master_catchup");
        WALManager masterWal = new WALManager(masterWalDir);
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key1".getBytes(), "val1".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key2".getBytes(), "val2".getBytes()));
        masterWal.appendAndFlush(new LogRecord(0L, LogRecord.OP_PUT, 1L, "key3".getBytes(), "val3".getBytes()));

        int port = findFreePort();
        try (Replicator replicator = new Replicator(masterWal, port)) {
            replicator.start();

            // 2. Setup slave with LSN 0
            File slaveWalDir = tempFolder.newFolder("slave_catchup");
            WALManager slaveWal = new WALManager(slaveWalDir);
            try (SlaveNode slave = new SlaveNode("localhost", port, slaveWal)) {
                slave.start();

                // 3. Wait for catch-up
                Thread.sleep(500);

                // 4. Assert slave received all records
                assertEquals(3, slave.getReceivedRecords().size());
                assertEquals(1, slave.getReceivedRecords().get(0).getLsn());
                assertEquals(2, slave.getReceivedRecords().get(1).getLsn());
                assertEquals(3, slave.getReceivedRecords().get(2).getLsn());
            }
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
