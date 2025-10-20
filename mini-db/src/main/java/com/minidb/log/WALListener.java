package com.minidb.log;

public interface WALListener {
    void onNewRecord(LogRecord record);
}
