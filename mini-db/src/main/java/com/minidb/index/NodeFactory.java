package com.minidb.index;

public class NodeFactory<K extends Comparable<K>, V> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final int order;

    public NodeFactory(Serializer<K> keySerializer, Serializer<V> valueSerializer, int order) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.order = order;
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
                LeafNode<K,V> leaf = new LeafNode<>(order, keySerializer, valueSerializer);
                leaf.pageId = pageId;
                leaf.deserialize(data);
                node = leaf;
                break;
            case 2:
                InternalNode<K,V> internal = new InternalNode<>(order, keySerializer, valueSerializer);
                internal.pageId = pageId;
                internal.deserialize(data);
                node = internal;
                break;
            default:
                throw new IllegalStateException("Unknown node type" + flag);
        }
        return node;
    }
}