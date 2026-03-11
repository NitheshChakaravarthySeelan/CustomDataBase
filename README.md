# Need to 
- Properly setup the master and the slave node

# Setup And Run 
- mvn clean install
# Start the server
- mvn exec:java -Dexec.mainClass="com.minidb.MiniDb"
# Example use case 
> INSERT INTO kv (key, value) VALUES ('hello', 'world')
OK
> SELECT * FROM kv WHERE key = 'hello'
hello -> world
> INSERT INTO kv (key, value) VALUES ('a', '1')
OK
> INSERT INTO kv (key, value) VALUES ('b', '2')
OK
> INSERT INTO kv (key, value) VALUES ('c', '3')
OK
> SELECT * FROM kv WHERE key BETWEEN 'a' AND 'b'
a -> 1
b -> 2

1. Project Goal and Overview

  mini-db is an educational project aimed at building a simplified relational database management system (RDBMS) from the ground up. Its primary goal is to demonstrate the
   fundamental concepts and mechanisms that power modern databases, including:

   * Data Storage and Retrieval: How data is physically stored on disk and efficiently accessed.
   * Indexing: Accelerating data lookups using B+ trees.
   * Transaction Management: Ensuring data integrity and consistency through ACID properties.
   * Concurrency Control: Managing simultaneous access to data to prevent conflicts.
   * Crash Recovery: Restoring the database to a consistent state after unexpected failures.
   * SQL Processing: Parsing and executing basic SQL commands.
   * Replication: Maintaining high availability and data durability across multiple instances.

  The project emphasizes a modular design, allowing each component to be understood and developed independently while integrating seamlessly into the larger system.

  ---

  2. Core Components and Their Low-Level Details

  2.1. Storage Layer (com.minidb.storage)

  This layer is responsible for the physical management of data on disk and in memory.

   * `Page.java`:
       * Purpose: Represents the fundamental unit of data storage and transfer between disk and memory. A page is a fixed-size block of bytes (e.g., 4KB).
       * Details: Contains a ByteBuffer to hold the page's raw data, a pageId (unique identifier), and a pinCount (for buffer pool management). It's the atomic unit for I/O 
         operations.
   * `PageHeader.java`:
       * Purpose: Stores metadata at the beginning of each Page, such as the page type, free space pointers, and LSNs.
       * Details: Serialized/deserialized to/from the start of a Page's ByteBuffer.
   * `PageManager.java`:
       * Purpose: Manages the allocation, deallocation, reading, and writing of pages to/from disk. It's the interface to the underlying file system.
       * Details: Interacts directly with FileChannel to perform read() and write() operations on database files. It assigns unique pageIds.
   * `BufferPool.java`:
       * Purpose: Caches frequently accessed Pages in memory to reduce disk I/O, improving performance. Implements a page replacement policy (e.g., LRU, not explicitly 
         defined but implied by pinCount).
       * Details: A fixed-size pool of Page objects. When a requested page is not in the pool (a "page fault"), it fetches it from PageManager. It "pins" pages in memory to 
         prevent them from being evicted while in use and "unpins" them when done. "Dirty" pages (modified in memory) are written back to disk before eviction.
   * `RecordStorage.java`:
       * Purpose: Manages the storage and retrieval of individual records within Pages.
       * Details: Divides pages into slots, each holding a record. It handles variable-length records, free space management within pages, and provides insert(), delete(), 
         update(), and get() operations for records. Returns RecordIds (pageId + slotId) for record identification.
   * `RecordId.java`:
       * Purpose: A unique identifier for a record, comprising its pageId and slotId within that page.

  2.2. Write-Ahead Log (WAL) and Recovery (com.minidb.log)

  Ensures durability and atomicity of transactions, and enables crash recovery.

   * `LogRecord.java`:
       * Purpose: Represents a single entry in the WAL, detailing a change made to the database.
       * Details: Contains lsn (Log Sequence Number), type (e.g., OP_PUT, OP_DELETE), txId (transaction ID), key, value, and a checksum. It provides serialize() and 
         deserialize() methods for converting to/from byte[].
   * `WALManager.java`:
       * Purpose: Manages the creation, appending, and flushing of LogRecords to the WAL file(s). It's central to crash recovery and replication.
       * Details:
           * Maintains nextLsn (an AtomicLong) to assign monotonically increasing LSNs.
           * append(LogRecord r): Appends a new LogRecord (assigning it a new LSN) to the in-memory buffer.
           * flush(): Forces all buffered log records to be written to disk (FileChannel.force(true)).
           * appendAndFlush(LogRecord r): Combines append() and flush().
           * `WALListener` Integration: Allows external components (like Replicator) to register as listeners. After appendAndFlush(), it notifies all registered listeners 
             about the newly flushed LogRecord.
           * `appendPredefined(LogRecord record)`: A crucial method for replication. It appends a LogRecord to the WAL using the lsn already present in the record object 
             (from the master). This ensures slaves maintain the master's LSN sequence. It also updates nextLsn to prevent future conflicts.
           * getChannelForRecovery(): Provides a FileChannel for reading the WAL for recovery or streaming.
           * getWalDir(): Returns the directory where WAL files are stored.
           * getNextLsn(): Returns the current nextLsn value.
   * `RecoveryManager.java`:
       * Purpose: Recovers the database to a consistent state by replaying WAL records after a crash.
       * Details: Reads WAL records from the WALManager and applies them to the BufferPool and RecordStorage in LSN order. (Implementation details not fully explored in this
          context, but its role is defined).

  2.3. Transaction Management (com.minidb.txn)

  Ensures ACID properties for database operations.

   * `Transaction.java`:
       * Purpose: Represents a single logical unit of work in the database.
       * Details: Holds a txId (transaction ID), status (e.g., RUNNING, COMMITTED, ABORTED), and manages its own read/write sets.
   * `TxnManager.java`:
       * Purpose: Manages the lifecycle of transactions, including starting, committing, and aborting them. Coordinates with WALManager and LockManager.
       * Details: Assigns txIds, ensures proper WAL logging for transaction boundaries, and interacts with the LockManager for concurrency control.
   * `LockManager.java`:
       * Purpose: Implements concurrency control by managing locks on database resources (e.g., pages, records).
       * Details: Grants and releases locks (shared/exclusive) to transactions, preventing conflicts. Uses a LockTable.
   * `LockTable.java`:
       * Purpose: Stores the current state of locks held by transactions on various resources.

  2.4. SQL Processing (com.minidb.sql)

  Handles parsing and execution of SQL commands.

   * `Tokenizer.java`:
       * Purpose: Breaks down a SQL query string into a stream of Tokens.
       * Details: Recognizes keywords, identifiers, operators, literals, etc.
   * `Parser.java`:
       * Purpose: Takes a stream of Tokens and constructs an Abstract Syntax Tree (AST) representing the SQL query.
       * Details: Implements grammar rules for SQL (e.g., SELECT, INSERT, DELETE).
   * `ast/*.java`:
       * Purpose: Classes representing nodes in the AST (e.g., SelectCommand, InsertCommand, Predicate).
   * `Executor.java`:
       * Purpose: Executes the commands represented by the AST.
       * Details: Translates AST nodes into operations on the RecordStorage and BPlusTree.

  2.5. Replication (com.minidb.replication) - Newly Implemented

  Provides master-slave log shipping for high availability and durability.

   * Core Concept: The master node streams its WAL entries to one or more slave nodes. Slaves replay these entries to maintain an identical copy of the master's data. This 
     is write-ahead-based, asynchronous, and recoverable.
   * `SyncProtocol.java`: (See 2.1) Defines message types and protocol version.
   * `transport/Message.java`: (See 2.2) Handles binary serialization/deserialization of messages with framing and CRC.
   * `transport/SocketChannel.java` & `transport/TcpSocketChannel.java`: (See 2.4) Network abstraction for master-slave communication.
   * `ReplicationState.java`: (See 2.5) Tracks the progress and health of each slave on the master.
   * `LogStreamer.java`: (See 2.6) On the master, reads LogRecords from the WAL file(s) starting from a given LSN, allowing for streaming of historical data to lagging 
     slaves.
   * `Replicator.java`:
       * Role: Master-side replication coordinator.
       * Details:
           * Listens for new LogRecords from WALManager (via WALListener).
           * Accepts incoming slave connections.
           * Handles HELLO messages from slaves, creating ReplicationState and initiating historical streaming if the slave is behind (using LogStreamer).
           * Sends new LogRecords to all active slaves as STREAM_WAL messages.
           * Processes ACK messages from slaves, updating their lastAckLSN in ReplicationState.
   * `SlaveNode.java`:
       * Role: Slave-side replication manager.
       * Details:
           * Connects to the master Replicator.
           * Sends HELLO with its lastAppliedLSN (loaded from persisted state).
           * Receives STREAM_WAL messages from the master.
           * Deserializes LogRecords from STREAM_WAL messages.
           * Applies these LogRecords to its local WALManager using appendPredefinedAndFlush(), preserving the master's LSNs.
           * Persists its lastAppliedLSN to slave_state.dat for crash recovery.
           * Sends ACK messages back to the master after successfully applying a batch of records.
           * Includes testability hooks like ackDelayMillis and recordsReceivedLatch.

  ---

  3. Key Design Principles/Philosophies

   * WAL-based Durability and Recovery: All changes are first written to the WAL before being applied to data pages. This is fundamental for crash recovery and forms the 
     basis of replication.
   * ACID Properties: The system aims to uphold Atomicity, Consistency, Isolation, and Durability through transactions, WAL, and concurrency control.
   * Concurrency Control: Implemented via a LockManager and LockTable to ensure correct execution of concurrent transactions.
   * Modular Design: Components are designed with clear responsibilities and interfaces, promoting maintainability and extensibility. For example, the SocketChannel 
     interface allows swapping network implementations.
   * LSN as Source of Truth: The Log Sequence Number (LSN) is a monotonically increasing identifier for LogRecords, serving as the primary mechanism for ordering events, 
     tracking replication progress, and ensuring recovery.
   * Asynchronous Replication: Master commits locally first, then asynchronously streams logs to slaves, prioritizing master performance.

  ---

  4. How to Run and Test (Summary)

  4.1. Building the Project

   1. Navigate to the project root: Open your terminal and change directory to mini-db/.
   2. Compile and package: Run mvn clean install. This command compiles all Java source files, runs unit tests, and packages the project into a JAR file (e.g., 
      target/mini-db-1.0-SNAPSHOT.jar).

  4.2. Running the System (Conceptual)

  To run the master and slave nodes, you would typically create a main application class (e.g., com.minidb.Main) that can be configured to start either a master or a slave
   instance.

   * Master Startup (Example Logic):

    1     // Inside com.minidb.Main.java
    2     public static void main(String[] args) throws Exception {
    3         // ... parse args to determine role (master/slave) and parameters ...
    4         if ("master".equals(role)) {
    5             File walDir = new File("./master_data/wal"); // Dedicated WAL directory for master
    6             walDir.mkdirs(); // Ensure directory exists
    7             WALManager masterWal = new WALManager(walDir);
    8             Replicator replicator = new Replicator(masterWal, 8080); // Master listens on port 8080
    9             replicator.start();
   10             System.out.println("Master Replicator started on port 8080. Press Enter to stop.");
   11             System.in.read(); // Keep application running
   12             replicator.close();
   13             masterWal.close();
   14         }
   15         // ... other roles ...
   16     }
      Command Line (Example): java -cp target/mini-db-1.0-SNAPSHOT.jar com.minidb.Main master

   * Slave Startup (Example Logic):

    1     // Inside com.minidb.Main.java
    2     public static void main(String[] args) throws Exception {
    3         // ... parse args ...
    4         if ("slave".equals(role)) {
    5             File walDir = new File("./slave_data/wal"); // Dedicated WAL directory for slave
    6             walDir.mkdirs(); // Ensure directory exists
    7             WALManager slaveWal = new WALManager(walDir);
    8             SlaveNode slave = new SlaveNode("localhost", 8080, slaveWal); // Connect to master
    9             slave.start();
   10             System.out.println("Slave Node started, connecting to localhost:8080. Press Enter to stop.");
   11             System.in.read(); // Keep application running
   12             slave.close();
   13             slaveWal.close();
   14         }
   15         // ... other roles ...
   16     }
      Command Line (Example): java -cp target/mini-db-1.0-SNAPSHOT.jar com.minidb.Main slave

  4.3. Testing the System

  All core functionalities, including the replication feature, are covered by JUnit tests.

   1. Run Tests: From the mini-db/ project root, execute mvn test.
   2. Test Suites:
       * `ReplicationFlowTest.java`: Verifies basic communication (handshake), master's WALListener integration, end-to-end replication of a single record (including ACK 
         processing), and a slave's ability to catch up on historical records.
       * `SlaveRecoveryTest.java`: Simulates a slave crashing mid-replication and verifies that upon restart, it correctly loads its last applied LSN from persisted state 
         and catches up on missed records.
       * `LagAndResyncTest.java`: Demonstrates replication lag by introducing a delay in the slave's ACK messages. It asserts that the master detects this lag and that the 
         slave eventually catches up once the delay is removed.
   3. Verification: A successful mvn test run (indicated by BUILD SUCCESS and Failures: 0, Errors: 0) confirms that all implemented components are working as designed and 
      integrated correctly.

  ---

  5. Future Directions and Improvements

  The current mini-db provides a solid foundation, but many areas can be expanded:

   * Query Optimizer: Implement a component to analyze and optimize SQL query execution plans.
   * More Data Types and Operators: Expand support for various data types (e.g., dates, floats) and SQL operators.
   * Advanced Concurrency Control: Explore more sophisticated concurrency control mechanisms beyond basic locking (e.g., MVCC - Multi-Version Concurrency Control).
   * Distributed Transactions: Implement protocols for transactions spanning multiple nodes.
   * Advanced Replication Features:
       * `LogReceiver` Refactoring: Extract the LogReceiver functionality from SlaveNode into a dedicated class for better modularity.
       * Efficient `LogStreamer`: Implement an LSN-to-file-offset index for LogStreamer to enable efficient seeking in large WAL files, avoiding full scans.
       * Heartbeats: Fully implement PING/PONG messages for robust liveness detection.
       * Flow Control: Implement more sophisticated flow control mechanisms to prevent master from overwhelming slow slaves.
       * Semi-Synchronous/Synchronous Replication: Introduce options for stronger consistency guarantees (e.g., master waits for at least one slave ACK before committing).
       * Quorum Replication / Raft Consensus: Evolve the replication to a fault-tolerant consensus-based system for higher availability and data consistency guarantees in 
         distributed environments.
       * Network Resilience: Implement more robust error handling for network failures, including exponential backoff and automatic reconnection strategies.
       * Security: Add TLS/SSL encryption and authentication for replication channels.
   * Storage Engine Enhancements: Implement B+ tree node splitting/merging, more advanced page replacement policies, and support for larger-than-memory databases.
