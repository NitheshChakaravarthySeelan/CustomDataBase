package com.minidb.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/*
 * Serializing: Convert a logical record or a custom Row class into a byte[] suitable to store in a Page.
 * Deserializing: Convert a byte[] back into a logical record.
 * Optional: Handle variable length fields, type encodings, and nulls.
 */
public class RecordsSerializer {
    // Will be a variable length field(String, byte[]) need to store a length prefix befoer the data
    class Row {
        Object[] value;
    }

    class Column {
        String name;
        ColumnType type;
        int maxLength;
        boolean nullable;
    }

    enum ColumnType {
        INT, LONG, STRING, BYTE_ARRAY
    }

    class RecordSerializer {
        Column[] columns;
    }

    public byte[] serialize(Row row) throws IOException {
        int numColumns = row.value.length;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] nullBitMap = new byte[(numColumns + 7)/8];

        // Build null bitmap
        for (int i=0; i<numColumns; i++) {
            if (row.value[i] != null) {
                nullBitMap[i/8] |= 1 << (i % 8);
            }
        }

        outputStream.write(nullBitMap);


        RecordSerializer serializer = new RecordSerializer();

        for (int i=0; i< numColumns; i++) {
            Object value = row.value[i];
            if (value == null) {
                continue;
            }

            Column column = serializer.columns[i];

            switch (column.type) {
                case INT:
                    ByteBuffer buf = ByteBuffer.allocate(4);
                    buf.putInt((Integer) row.value[i]);
                    outputStream.write(buf.array());
                    break;
                case LONG:
                    buf = ByteBuffer.allocate(8);
                    buf.putLong((Long) row.value[i]);
                    outputStream.write(buf.array());
                    break;
                case STRING:
                    byte[] strBytes = ((String) row.value[i]).getBytes(StandardCharsets.UTF_8);
                    buf = ByteBuffer.allocate(4);
                    buf.putInt(strBytes.length);  // write length prefix
                    outputStream.write(buf.array());
                    outputStream.write(strBytes);
                    break;
                case BYTE_ARRAY:
                    byte[] data = (byte[]) row.value[i];
                    buf = ByteBuffer.allocate(4);
                    buf.putInt(data.length);
                    outputStream.write(buf.array());
                    outputStream.write(data);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown column type ");
            }
        }
        return outputStream.toByteArray();
    }

    public Row deserialize(byte[] recordBytes) {
        int offset = 0;
        RecordSerializer serializer = new RecordSerializer();
        Row row = new Row();
        int numColumns = serializer.columns.length;

        for (int i=0; i< numColumns; i++) {
            Column column = serializer.columns[i];

            
            
    }

}