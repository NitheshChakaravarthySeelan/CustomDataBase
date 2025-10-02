package com.minidb.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {
    protected List<Node<K, V>> children;
    private final Serializer<K> keySerializer;
    private final Serializer<V> childSerializer;
    protected LeafNode<K, V> nextNode;

    public InternalNode(int order, Serializer<K> keySer, Serializer<V> childSer) {
        this.order = order;
        this.keys = new ArrayList<>(order);
        this.children = new ArrayList<>(order + 1);
        this.isLeaf = false;
        this.keySerializer = keySer;
        this.childSerializer = childSer;
    }

    public SplitResult<K, ? extends Node<K, V>> insert(K key, V value) {
        int pos = findChildPosition(key);
        SplitResult<K, ? extends Node<K, V>> splitResult = children.get(pos).insert(key, value);
        if (splitResult == null) {
            return null;
        }
        keys.add(pos, splitResult.getSplitKey());
        children.add(pos + 1, splitResult.getRightNode());
        if (keys.size() < order) {
            return null;
        }
        return split();
    }

    private SplitResult<K, InternalNode<K, V>> split() {
        int mid = keys.size() / 2;
        K upKey = keys.get(mid);
        InternalNode<K, V> rightNode = new InternalNode<>(order, keySerializer, childSerializer);

        rightNode.keys.addAll(keys.subList(mid+1, keys.size()));
        rightNode.children.addAll(children.subList(mid + 1, children.size()));
        keys = new ArrayList<>(keys.subList(0, mid));
        children = new ArrayList<>(children.subList(0, mid + 1));

        return new SplitResult<>(upKey, rightNode);
    }

    @Override
    public V search(K key) {
        int pos = findChildPosition(key);
        return children.get(pos).search(key);
    }

    int findChildPosition(K key) {
        int left = 0;
        int right = keys.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = key.compareTo(keys.get(mid));
            if (cmp > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return left;
    }

    @Override
    public byte[] serialize() {
        // Each node = header + keys + children pointers
        // We'll store children as their pageId
        ByteBuffer buffer = ByteBuffer.allocate(4096); // fixed page size for simplicity

        buffer.put((byte)0); // 0 = internal, 1 = leaf
        buffer.putInt(keys.size());

        // Write keys
        for (K key : keys) {
            byte[] keyBytes = keySerializer.serialize(key);
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
        }

        // Write children pageIds
        for (Node<K, V> child : children) {
            buffer.putInt(child.pageId);
        }

        return buffer.array();
    }

    @Override
    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte flag = buffer.get();
        if (flag != 0) throw new IllegalStateException("Not an internal node");

        int numKeys = buffer.getInt();

        keys = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            int keyLen = buffer.getInt();
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            K key = keySerializer.deserialize(keyBytes);
            keys.add(key);
        }

        children = new ArrayList<>(numKeys + 1);
        for (int i = 0; i < numKeys + 1; i++) {
            int childPageId = buffer.getInt();
            InternalNode<K,V> placeholder = new InternalNode<>(order, keySerializer, childSerializer); 
            placeholder.pageId = childPageId;
            children.add(placeholder);
        }
    }

}
