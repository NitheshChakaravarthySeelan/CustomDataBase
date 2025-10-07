package com.minidb.serializers;

import com.minidb.index.Serializer;
import java.nio.ByteBuffer;

public class IntegerSerializer implements Serializer<Integer> {
    @Override
    public byte[] serialize(Integer obj) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(obj);
        return buffer.array();
    }

    @Override
    public Integer deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt();
    }

    @Override
    public int getSerializedSize(Integer obj) {
        return 4;
    }
}
