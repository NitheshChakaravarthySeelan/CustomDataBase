package com.minidb.serializers;

import com.minidb.index.Serializer;

import java.nio.ByteBuffer;

public class IntegerSerializer implements Serializer<Integer> {
    @Override
    public byte[] serialize(Integer obj) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(obj);
        return buffer.array();
    }

    @Override
    public Integer deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getInt();
    }

    @Override
    public int getSerializedSize(Integer obj) {
        return Integer.BYTES;
    }
}
