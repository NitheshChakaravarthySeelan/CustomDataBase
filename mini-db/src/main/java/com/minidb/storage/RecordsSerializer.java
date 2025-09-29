package com.minidb.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*
 * Serializing: Convert a logical record or a custom Row class into a byte[] suitable to store in a Page.
 * Deserializing: Convert a byte[] back into a logical record.
 * Optional: Handle variable length fields, type encodings, and nulls.
 */
public class RecordsSerializer {
    private Column[] columns;
    private final int numColumns;
    private final int nullBitmapSize;

    // Will be a variable length field(String, byte[]) need to store a length prefix befoer the data
    public static class Row {
        public final Object[] values;

        public Row(int numColumns) {
            this.values = new Object[numColumns];
        }
    }

    public static class Column {
        String name;
        ColumnType type;
        int maxLength;
        boolean nullable;
    }

    public enum ColumnType {
        INT, LONG, STRING, BYTE_ARRAY
    }

    public RecordsSerializer(Column[] columns) {
        this.columns = columns;
        this.numColumns = columns.length;
        this.nullBitmapSize = (this.numColumns + 7) / 8;
    }

    public byte[] serialize(Row row) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] nullBitMap = new byte[nullBitmapSize];

        // Build null bitmap
        for (int i=0; i<numColumns; i++) {
            if (row.values[i] != null) {
                nullBitMap[i/8] |= 1 << (i % 8);
            }
        }
        
        // Adding the null bitmap to the byte array which should be accessed before the actual column values.
        outputStream.write(nullBitMap);

        for (int i=0; i< numColumns; i++) {
            Object value = row.values[i];
            if (value == null) {
                continue;
            }

            Column column = this.columns[i];

            switch (column.type) {
                case INT:
                    ByteBuffer buf = ByteBuffer.allocate(4);
                    buf.putInt((Integer) value);
                    outputStream.write(buf.array());
                    break;
                case LONG:
                    buf = ByteBuffer.allocate(8);
                    buf.putLong((Long) value);
                    outputStream.write(buf.array());
                    break;
                case STRING:
                    byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    buf = ByteBuffer.allocate(4);
                    buf.putInt(strBytes.length);  // write length prefix
                    outputStream.write(buf.array());
                    outputStream.write(strBytes);
                    break;
                case BYTE_ARRAY:
                    byte[] data = (byte[]) value;
                    buf = ByteBuffer.allocate(4);
                    buf.putInt(data.length);
                    outputStream.write(buf.array());
                    outputStream.write(data);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown column type: " + column.type);
            }
        }
        return outputStream.toByteArray();
    }

    public Row deserialize(byte[] recordBytes) {
        Row row = new Row(numColumns);
        int offset = nullBitmapSize; // Data starts after the null bitmap

        // Read the null bitmap to determine which fields are present
        for (int i=0; i< numColumns; i++) {
            // Check if the bit for this column is set in the bitmap
            if (((recordBytes[i/8] >> (i % 8)) & 1) == 0) {
                // Bit is 0, so the value is null
                row.values[i] = null;
            } else {
                // Bit is 1, so the value is present. Deserialize it.
                Column column = this.columns[i];
                switch (column.type) {
                    case INT:
                        // read 4 bytes
                        int intValue = ByteBuffer.wrap(recordBytes, offset, 4).getInt();
                        row.values[i] = intValue;
                        offset += 4;
                        break;

                    case LONG:
                        long longValue = ByteBuffer.wrap(recordBytes, offset, 8).getLong();
                        row.values[i] = longValue;
                        offset += 8;
                        break;

                    case STRING:
                        // first read length (4 bytes)
                        int strLen = ByteBuffer.wrap(recordBytes, offset, 4).getInt();
                        offset += 4;

                        // read string bytes
                        String strValue = new String(recordBytes, offset, strLen, StandardCharsets.UTF_8);
                        row.values[i] = strValue;
                        offset += strLen;
                        break;

                    case BYTE_ARRAY:
                        int byteLen = ByteBuffer.wrap(recordBytes, offset, 4).getInt();
                        offset += 4;

                        byte[] data = Arrays.copyOfRange(recordBytes, offset, offset + byteLen);
                        row.values[i] = data;
                        offset += byteLen;
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown column type: " + column.type);
                }
            }
        }
        return row;
    }
}