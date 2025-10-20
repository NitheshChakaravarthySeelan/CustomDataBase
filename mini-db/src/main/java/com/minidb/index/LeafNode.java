package com.minidb.index;

import com.minidb.index.Node.SplitResult;
import com.minidb.storage.BufferPool;
import com.minidb.storage.Page;
import com.minidb.storage.PageManager;
import com.minidb.storage.RecordId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
    final List<V> values;
    LeafNode<K, V> nextNode;

    public LeafNode(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer, BufferPool bufferPool) {
        super(order, new ArrayList<>(), keySerializer, valueSerializer, bufferPool);
        this.values = new ArrayList<>();
    }

    @Override
    public void writeNode() throws IOException {
        Page page = bufferPool.getPage(pageId);
        byte[] serializedData = serialize();
        System.arraycopy(serializedData, 0, page.toBytes(), 0, serializedData.length);
        page.setDirty(true);
        bufferPool.unpinPage(pageId, true);
    }

    @Override
    public void readNode() throws IOException {
        Page page = bufferPool.getPage(pageId);
        deserialize(page.toBytes());
        bufferPool.unpinPage(pageId, false);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public V search(K key) {
        int index = Collections.binarySearch(keys, key, Comparator.naturalOrder());
        return index >= 0 ? values.get(index) : null;
    }

    @Override
    public SplitResult<K, LeafNode<K, V>> insert(K key, V value) {
        System.out.println("LeafNode.insert: This hashcode: " + this.hashCode());
        int index = Collections.binarySearch(keys, key, Comparator.naturalOrder());
        int insertionPoint = (index >= 0) ? index : -index - 1;

        if (index >= 0) { // Key exists, update value
            values.set(insertionPoint, value);
        } else { // Key does not exist, insert new
            keys.add(insertionPoint, key);
            values.add(insertionPoint, value);
        }

        if (keys.size() < order) {
            return null; // No split needed
        }

        // Split the node
        int mid = keys.size() / 2;
        LeafNode<K, V> rightNode = new LeafNode<>(order, keySerializer, valueSerializer, bufferPool);

        rightNode.keys.addAll(keys.subList(mid, keys.size()));
        rightNode.values.addAll(values.subList(mid, values.size()));

        // Clear the second half from the current node
        keys.subList(mid, keys.size()).clear();
        values.subList(mid, values.size()).clear();

        // Link the leaf nodes
        rightNode.nextNode = this.nextNode;
        this.nextNode = rightNode;

        return new SplitResult<>(rightNode.getFirstKey(), rightNode);
    }

    @Override
    public void delete(K key) {
        int index = Collections.binarySearch(keys, key, Comparator.naturalOrder());
        if (index < 0) {
            return; // Key not found
        }

        keys.remove(index);
        values.remove(index);

        if (parent != null && isUnderflow()) {
            parent.handleUnderflow(this, key);
        }
    }

    @Override
    public K getFirstKey() {
        return keys.isEmpty() ? null : keys.get(0);
    }

    // Called by parent to give this node a key from a sibling
    void borrowFromLeft(LeafNode<K, V> leftSibling, K parentKey) {
        K borrowedKey = leftSibling.keys.remove(leftSibling.keyCount() - 1);
        V borrowedValue = leftSibling.values.remove(leftSibling.keyCount());
        this.keys.add(0, borrowedKey);
        this.values.add(0, borrowedValue);
        parent.updateKey(parentKey, borrowedKey);
    }

    void borrowFromRight(LeafNode<K, V> rightSibling, K parentKey) {
        K borrowedKey = rightSibling.keys.remove(0);
        V borrowedValue = rightSibling.values.remove(0);
        this.keys.add(borrowedKey);
        this.values.add(borrowedValue);
        parent.updateKey(parentKey, rightSibling.getFirstKey());
    }

    // Called by parent to merge this node with a sibling
    void mergeWithLeft(LeafNode<K, V> leftSibling, K parentKey) {
        leftSibling.keys.addAll(this.keys);
        leftSibling.values.addAll(this.values);
        leftSibling.nextNode = this.nextNode;
        parent.removeChild(parentKey, this);
    }

    public void mergeWithRight(LeafNode<K, V> rightSibling, K parentKey) {
        this.keys.addAll(rightSibling.keys);
        this.values.addAll(rightSibling.values);
        this.nextNode = rightSibling.nextNode;
        parent.removeChild(parentKey, rightSibling);
    }

    @Override
    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.get(); // Skip node type byte
        int numKeys = buffer.getInt();

        for (int i = 0; i < numKeys; i++) {
            int keySize = buffer.getInt();
            byte[] keyBytes = new byte[keySize];
            buffer.get(keyBytes);
            K key = keySerializer.deserialize(keyBytes);
            keys.add(key);

            int valueSize = buffer.getInt();
            byte[] valueBytes = new byte[valueSize];
            buffer.get(valueBytes);
            V value = valueSerializer.deserialize(valueBytes);
            values.add(value);
        }
    }

    public byte[] serialize() {
        // Calculate total size needed
        int size = 1 + 4; // 1 byte for node type, 4 bytes for num_keys
        for (int i = 0; i < keys.size(); i++) {
            int keySize = keySerializer.getSerializedSize(keys.get(i));
            int valueSize = valueSerializer.getSerializedSize(values.get(i));
            size += 4 + keySize; // 4 bytes for key size
            size += 4 + valueSize; // 4 bytes for value size
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put((byte) 1); // LeafNode type
        buffer.putInt(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            byte[] keyBytes = keySerializer.serialize(keys.get(i));
            byte[] valueBytes = valueSerializer.serialize(values.get(i));
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);
        }
        return buffer.array();
    }
}