package com.minidb.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
    final List<V> values;
    LeafNode<K, V> nextNode;

    public LeafNode(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(order, new ArrayList<>(), keySerializer, valueSerializer);
        this.values = new ArrayList<>();
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public V search(K key) {
        int index = Collections.binarySearch(keys, key);
        return index >= 0 ? values.get(index) : null;
    }

    @Override
    public SplitResult<K, LeafNode<K, V>> insert(K key, V value) {
        int index = Collections.binarySearch(keys, key);
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
        LeafNode<K, V> rightNode = new LeafNode<>(order, keySerializer, valueSerializer);

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
        int index = Collections.binarySearch(keys, key);
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
        // Implementation depends on the serialization format
    }
}