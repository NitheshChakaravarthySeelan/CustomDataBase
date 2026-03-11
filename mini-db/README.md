# MiniDB: A From-Scratch Relational Database Engine

**MiniDB** is a lightweight, educational relational database management system (RDBMS) built in Java. It implements the core components of a modern database, providing a deep dive into storage internals, transaction management, and distributed consensus.

---

## 🚀 Quick Start

### 1. Build the Project
Ensure you have Maven installed, then run:
```bash
mvn clean compile
```

### 2. Start the Database
Use the provided `minidb` script to start a node. The script automatically handles the classpath and dependencies. 
**Usage:** `./minidb <node_id> <peer_ids>`
```bash
./minidb node1 node1
```

---

## ⚙️ Configuration

MiniDB is configured via `src/main/resources/application.properties`.

| Property | Default | Description | Impact of Changing |
| :--- | :--- | :--- | :--- |
| `minidb.pageSize` | `4096` | Size of a single data block on disk (in bytes). | **Larger:** Better sequential reads. **Smaller:** Lower memory per page. |
| `minidb.bufferPoolSize` | `10` | Number of pages in the LRU cache. | **Higher:** Fewer disk reads. **Lower:** Lower memory footprint. |
| `minidb.bPlusTreeOrder` | `5` | Max keys per B+ Tree node. | **Higher:** Flatter tree (faster search). **Lower:** Faster splits/merges. |

---

## 🛠️ Testing & Commands

**Note:** The SQL parser is strict. Please enter commands **without** a trailing semicolon.

### 1. Basic SQL Operations
```sql
# Insert records
INSERT INTO kv (id, value) VALUES (1, 'database')
INSERT INTO kv (id, value) VALUES (2, 'storage')

# Point lookup
SELECT * FROM kv WHERE id = 1
# Output: 1 -> database

# Deletion
DELETE FROM kv WHERE id = 1
OK
```

### 2. Testing Durability (The "Crash" Test)
1. Start the DB: `./minidb node1 node1`
2. Run: `INSERT INTO kv (id, value) VALUES (100, 'important_data')`
3. Exit: `.exit`
4. Restart: `./minidb node1 node1`
5. Verify: `SELECT * FROM kv WHERE id = 100` → **Data is restored from the WAL!**

---

## 🏗️ Architecture (Deep Dive for Interviews)

- **Storage Layout:** 
  - **Page 0:** Reserved for file metadata (Signature, Page Size).
  - **Page 1:** Reserved for the B+ Tree Root node.
  - **Page 2+:** Allocated for data records.
- **Slotted Pages:** Each page uses a slot directory at the end of the buffer to manage variable-length records and reclaim space via compaction.
- **Page Type Awareness:** The system differentiates between Data, Leaf, and Internal pages in the header to prevent corruption.
- **ACID Transactions:** Uses **Strict 2PL** (Locking) and **WAL** (Logging). Recovery is performed by replaying "DONE" log records to ensure only committed transactions are applied.
- **Consensus:** Integrated with the **Raft algorithm** to replicate the log across nodes.

---

## 🔮 Future Roadmap
1. **Schema Support:** Multi-column tables and custom data types.
2. **MVCC:** Non-blocking reads for improved concurrency.
3. **Log Compaction:** Raft snapshotting to manage log size.
4. **Range Scans:** Full implementation of leaf node linking for `BETWEEN` queries.
