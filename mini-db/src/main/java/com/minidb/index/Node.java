package com.minidb.index;

import java.util.List;

public abstract class Node<K extends Comparable<K>, V> {
    protected final int order;
    protected final List<K> keys;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;
    protected InternalNode<K, V> parent;

    public Node(int order, List<K> keys, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.order = order;
        this.keys = keys;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public int keyCount() {
        return keys.size();
    }

    public abstract boolean isLeaf();

    public abstract V search(K key);

    public abstract SplitResult<K, ? extends Node<K, V>> insert(K key, V value);

    public abstract void delete(K key);

    public abstract K getFirstKey();

    /**
     * A node is under-full if it has fewer than ceil(order/2) - 1 keys.
     * For simplicity, we'll use (order-1)/2.
     */
    protected boolean isUnderflow() {
        return keys.size() < (order - 1) / 2;
    }

    public static class SplitResult<K extends Comparable<K>, N extends Node<K, ?>> {
        private final K splitKey;
        private final N rightNode;

        public SplitResult(K splitKey, N rightNode) {
            this.splitKey = splitKey;
            this.rightNode = rightNode;
        }
        public K getSplitKey() { return splitKey; }
        public N getRightNode() { return rightNode; }
    }
}