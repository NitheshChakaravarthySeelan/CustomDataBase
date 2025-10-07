package com.minidb.index;

public interface Serializer<T> {
    public byte[] serialize(T obj);
    public T deserialize(byte[] data);
    int getSerializedSize(T obj);
}