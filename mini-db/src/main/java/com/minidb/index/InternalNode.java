package com.minidb.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {
    final List<Node<K, V>> children;

    public InternalNode(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(order, new ArrayList<>(), keySerializer, valueSerializer);
        this.children = new ArrayList<>();
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public V search(K key) {
        return children.get(findChildPosition(key)).search(key);
    }

    @Override
    public SplitResult<K, InternalNode<K, V>> insert(K key, V value) {
        int childIndex = findChildPosition(key);
        Node<K, V> child = children.get(childIndex);
        SplitResult<K, ? extends Node<K, V>> result = child.insert(key, value);

        if (result == null) {
            return null; // No split occurred in child
        }

        // A split occurred, so we must insert the new key and child into this node
        int keyIndex = Collections.binarySearch(keys, result.getSplitKey());
        int insertionPoint = (keyIndex >= 0) ? keyIndex : -keyIndex - 1;
        keys.add(insertionPoint, result.getSplitKey());
        children.add(insertionPoint + 1, result.getRightNode());
        result.getRightNode().parent = this;

        if (keys.size() < order) {
            return null; // This node did not split
        }

        // This node is full, split it
        int mid = keys.size() / 2;
        InternalNode<K, V> rightNode = new InternalNode<>(order, keySerializer, valueSerializer);
        K splitKey = keys.remove(mid);

        rightNode.keys.addAll(keys.subList(mid, keys.size()));
        rightNode.children.addAll(children.subList(mid + 1, children.size()));
        rightNode.children.forEach(c -> c.parent = rightNode);

        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();

        return new SplitResult<>(splitKey, rightNode);
    }

    @Override
    public void delete(K key) {
        children.get(findChildPosition(key)).delete(key);
    }

    @Override
    public K getFirstKey() {
        return keys.get(0);
    }

    int findChildPosition(K key) {
        int index = Collections.binarySearch(keys, key);
        return (index >= 0) ? index + 1 : -index - 1;
    }

    void updateKey(K oldKey, K newKey) {
        int index = Collections.binarySearch(keys, oldKey);
        if (index >= 0) {
            keys.set(index, newKey);
        } else {
            // This can happen when the key is the implicit "less than all" key
            // and we need to update the first key in the parent.
            if (parent != null) {
                parent.updateKey(oldKey, newKey);
            }
        }
    }

    void removeChild(K key, Node<K, V> child) {
        int keyIndex = Collections.binarySearch(keys, key);
        if (keyIndex >= 0) {
            keys.remove(keyIndex);
            children.remove(child);
        } else {
            // Key not found, must be the rightmost child
            int childIndex = children.indexOf(child);
            if (childIndex > 0) {
                keys.remove(childIndex - 1);
                children.remove(childIndex);
            }
        }

        if (parent != null && isUnderflow()) {
            parent.handleUnderflow(this, key);
        }
    }

    void handleUnderflow(Node<K, V> child, K key) {
        int childIndex = children.indexOf(child);

        // Try to borrow from left sibling
        if (childIndex > 0) {
            Node<K, V> leftSibling = children.get(childIndex - 1);
            if (leftSibling.keyCount() > (order - 1) / 2) {
                // Redistribute
                if (child.isLeaf()) {
                    ((LeafNode<K, V>) child).borrowFromLeft((LeafNode<K, V>) leftSibling, keys.get(childIndex - 1));
                } else {
                    ((InternalNode<K, V>) child).borrowFromLeft((InternalNode<K, V>) leftSibling, keys.get(childIndex - 1));
                }
                return;
            }
        }

        // Try to borrow from right sibling
        if (childIndex < children.size() - 1) {
            Node<K, V> rightSibling = children.get(childIndex + 1);
            if (rightSibling.keyCount() > (order - 1) / 2) {
                // Redistribute
                if (child.isLeaf()) {
                    ((LeafNode<K, V>) child).borrowFromRight((LeafNode<K, V>) rightSibling, keys.get(childIndex));
                } else {
                    ((InternalNode<K, V>) child).borrowFromRight((InternalNode<K, V>) rightSibling, keys.get(childIndex));
                }
                return;
            }
        }

        // Cannot borrow, must merge
        if (childIndex > 0) {
            // Merge with left sibling
            Node<K, V> leftSibling = children.get(childIndex - 1);
            if (child.isLeaf()) {
                ((LeafNode<K, V>) child).mergeWithLeft((LeafNode<K, V>) leftSibling, keys.get(childIndex - 1));
            } else {
                ((InternalNode<K, V>) child).mergeWithLeft((InternalNode<K, V>) leftSibling, keys.get(childIndex - 1));
            }
        } else {
            // Merge with right sibling
            Node<K, V> rightSibling = children.get(childIndex + 1);
            if (child.isLeaf()) {
                ((LeafNode<K, V>) child).mergeWithRight((LeafNode<K, V>) rightSibling, keys.get(childIndex));
            } else {
                ((InternalNode<K, V>) child).mergeWithRight((InternalNode<K, V>) rightSibling, keys.get(childIndex));
            }
        }
    }

    // Internal node redistribution/merge logic
    void borrowFromLeft(InternalNode<K, V> leftSibling, K parentKey) {
        K borrowedKey = leftSibling.keys.remove(leftSibling.keyCount() - 1);
        Node<K, V> borrowedChild = leftSibling.children.remove(leftSibling.children.size() - 1);
        this.keys.add(0, parentKey);
        this.children.add(0, borrowedChild);
        borrowedChild.parent = this;
        parent.updateKey(parentKey, borrowedKey);
    }

    void borrowFromRight(InternalNode<K, V> rightSibling, K parentKey) {
        K borrowedKey = rightSibling.keys.remove(0);
        Node<K, V> borrowedChild = rightSibling.children.remove(0);
        this.keys.add(parentKey);
        this.children.add(borrowedChild);
        borrowedChild.parent = this;
        parent.updateKey(parentKey, borrowedKey);
    }

    void mergeWithLeft(InternalNode<K, V> leftSibling, K parentKey) {
        leftSibling.keys.add(parentKey);
        leftSibling.keys.addAll(this.keys);
        leftSibling.children.addAll(this.children);
        this.children.forEach(c -> c.parent = leftSibling);
        parent.removeChild(parentKey, this);
    }

    void mergeWithRight(InternalNode<K, V> rightSibling, K parentKey) {
        this.keys.add(parentKey);
        this.keys.addAll(rightSibling.keys);
        this.children.addAll(rightSibling.children);
        rightSibling.children.forEach(c -> c.parent = this);
        parent.removeChild(parentKey, rightSibling);
    }
}