package com.minidb.serializers;

import com.minidb.index.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public byte[] serialize(String obj) {
        return obj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public int getSerializedSize(String obj) {
        return obj.getBytes(StandardCharsets.UTF_8).length;
    }
}
