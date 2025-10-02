package com.minidb.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
    protected List<V> values;
    protected LeafNode<K, V> nextNode;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;


    public LeafNode(int order, Serializer<K> keySer, Serializer<V> valSer) {
        this.order = order;
        this.keys = new ArrayList<>(order);
        this.values = new ArrayList<>(order);
        this.keySerializer = keySer;
        this.valueSerializer = valSer;
        this.isLeaf = true;
    }

    @Override
    public SplitResult<K, ? extends Node<K, V>> insert(K key, V value) {
        int pos = findInsertPosition(key);
        keys.add(pos, key);
        values.add(pos, value);

        if (keys.size() < order) {
            return null; // No split
        }

        int mid = keys.size() / 2;
        LeafNode<K, V> rightNode = new LeafNode<>(order,keySerializer, valueSerializer);
        
        // Copy second half to the right node
        rightNode.keys.addAll(keys.subList(mid, keys.size()));
        rightNode.values.addAll(values.subList(mid, values.size()));

        // Keep left half in current node
        keys = new ArrayList<>(keys.subList(0, mid));
        values = new ArrayList<>(values.subList(0, mid));

        
        // Update linked list pointer
        rightNode.nextNode = nextNode;
        nextNode = rightNode;

        // Promote the first key of the right node as parent
        K splitKey = rightNode.keys.get(0);

        return new SplitResult<>(splitKey, rightNode);
    }

    @Override
    public V search(K key) {
        int left = 0;
        int right = keys.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = key.compareTo(keys.get(mid));
            if (cmp == 0) {
                return values.get(mid);
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return null;
    }

    @Override
    public byte[] serialize() {
        int totalSize = 4 + 4; // 4 for numKeys and 4 for nextNode
        List<byte[]> keyBytes = new ArrayList<>(keys.size());
        List<byte[]> valueBytes = new ArrayList<>(values.size());
        for (int i=0; i<keys.size(); i++) {
            keyBytes.add(keySerializer.serialize(keys.get(i)));
            valueBytes.add(valueSerializer.serialize(values.get(i)));
            totalSize += 4 + 4 + keyBytes.get(i).length + valueBytes.get(i).length;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            byte[] kBytes = keyBytes.get(i);
            byte[] vBytes = valueBytes.get(i);
            buffer.putInt(kBytes.length);
            buffer.put(kBytes);
            buffer.putInt(vBytes.length);
            buffer.put(vBytes);
        }

        buffer.putInt(nextNode != null ? nextNode.pageId : -1);
        return buffer.array();
    }

    @Override
    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int numKeys = buffer.getInt();
        keys.clear();
        values.clear();

        for (int i = 0; i < numKeys; i++) {
            int keyLen = buffer.getInt();
            byte[] kBytes = new byte[keyLen];
            buffer.get(kBytes);

            int valLen = buffer.getInt();
            byte[] vBytes = new byte[valLen];
            buffer.get(vBytes);

            keys.add(keySerializer.deserialize(kBytes));
            values.add(valueSerializer.deserialize(vBytes));
        }

        int nextNodeId = buffer.getInt();
        if (nextNodeId != -1) {
            nextNode = new LeafNode<>(order, keySerializer, valueSerializer);
            nextNode.pageId = nextNodeId;
        } else {
            nextNode = null;
        }
    }   

    private int findInsertPosition(K key) {
        int left = 0;
        int right = keys.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = ((Comparable<K>) key).compareTo(keys.get(mid));
            if (cmp > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return left;
    }
}
