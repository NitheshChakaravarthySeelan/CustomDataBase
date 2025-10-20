package com.minidb.index;

import com.minidb.storage.BufferPool;

public class NodeFactory<K extends Comparable<K>, V> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final int order;
    private final BufferPool bufferPool;

    public NodeFactory(Serializer<K> keySerializer, Serializer<V> valueSerializer, int order, BufferPool bufferPool) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.order = order;
        this.bufferPool = bufferPool;
    }

    /**
     * Given raw bytes and pageId, reconstruct the right Node
     * Leaf or Internal
     */
    public Node<K,V> fromBytes(byte[] data, int pageId) {
        byte flag = data[0];
        Node<K,V> node;

        switch (flag) {
            case 1:
                LeafNode<K,V> leaf = new LeafNode<>(order, keySerializer, valueSerializer, bufferPool);
                leaf.pageId = pageId;
                leaf.deserialize(data);
                node = leaf;
                break;
            case 2:
                InternalNode<K,V> internal = new InternalNode<>(order, keySerializer, valueSerializer, bufferPool);
                internal.pageId = pageId;
                internal.deserialize(data);
                node = internal;
                break;
            default:
                throw new IllegalStateException("Unknown node type" + flag);
        }
        return node;
    }

    public LeafNode<K, V> createLeafNode() {
        return new LeafNode<>(order, keySerializer, valueSerializer, bufferPool);
    }

    public InternalNode<K, V> createInternalNode() {
        return new InternalNode<>(order, keySerializer, valueSerializer, bufferPool);
    }
}