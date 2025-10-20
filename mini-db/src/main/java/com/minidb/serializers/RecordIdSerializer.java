package com.minidb.serializers;

import com.minidb.index.Serializer;
import com.minidb.storage.RecordId;

public class RecordIdSerializer implements Serializer<RecordId> {
    @Override
    public byte[] serialize(RecordId obj) {
        return obj.serialize();
    }

    @Override
    public RecordId deserialize(byte[] bytes) {
        return RecordId.deserialize(bytes);
    }

    @Override
    public int getSerializedSize(RecordId obj) {
        return RecordId.getSerializedSize();
    }
}
