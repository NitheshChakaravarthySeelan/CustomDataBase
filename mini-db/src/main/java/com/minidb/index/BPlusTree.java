package com.minidb.index;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BPlusTree<K extends Comparable<K>, V> {
    private Node<K, V> root;
    private final int order; // Maximum number of keys in a node
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public BPlusTree(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.order = order;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.root = new LeafNode<>(order, keySerializer, valueSerializer);
    }

    public void insert(K key, V value) {
        Node.SplitResult<K, ? extends Node<K, V>> result = root.insert(key, value);
        if (result != null) {
            InternalNode<K, V> newRoot = new InternalNode<>(order, keySerializer, valueSerializer);
            newRoot.keys.add(result.getSplitKey());
            newRoot.children.add(root);
            newRoot.children.add(result.getRightNode());
            root = newRoot;
        }
    }

    public void delete(K key) {
        root.delete(key);
        if (root instanceof InternalNode && root.keyCount() == 0) {
            root = ((InternalNode<K, V>) root).children.get(0);
        }
    }

    public V search(K key) {
        return root.search(key);
    }

    public List<Map.Entry<K, V>> rangeSearch(K startKey, K endKey) {
        List<Map.Entry<K, V>> results = new ArrayList<>();
        Node<K, V> node = root;

        // Step 1: descend to the correct leaf node
        while (!node.isLeaf()) {
            InternalNode<K, V> internalNode = (InternalNode<K, V>) node;
            int pos = internalNode.findChildPosition(startKey);
            node = internalNode.children.get(pos);
        }

        // Step 2: iterate through leaf nodes until we pass endKey
        LeafNode<K, V> leafNode = (LeafNode<K, V>) node;
        while (leafNode != null) {
            for (int i = 0; i < leafNode.keys.size(); i++) {
                K key = leafNode.keys.get(i);

                if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                    results.add(new AbstractMap.SimpleEntry<>(key, leafNode.values.get(i)));
                }

                if (key.compareTo(endKey) > 0) {
                    return results; // stop completely when endKey is exceeded
                }
            }
            leafNode = leafNode.nextNode;
        }
        return results;
    }

}