package com.minidb.index;

import java.util.List;

// This is basically a abstract implementation
public abstract class Node<K, V> {
    // Unique Id from pageManager 
    protected int pageId;
    protected List<K> keys;
    protected boolean isLeaf;
    protected int order;

    public static class SplitResult<K, N extends Node<K,?>> {
        public final K splitKey;
        public final N rightNode;

        public SplitResult(K splitKey, N rightNode) {
            this.splitKey = splitKey;
            this.rightNode = rightNode;
        }

        public K getSplitKey() {
            return splitKey;
        }

        public N getRightNode() {
            return rightNode;
        }
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public int keyCount() {
        return keys.size();
    }

    public boolean isOverflow() {
        return keyCount() >= order;
    }

    boolean isUnderflow() {
        return keyCount() < order / 2;
    }

    public abstract SplitResult<K, ? extends Node<K, V>>insert(K key, V value);

    // Recursive search down the tree
    public abstract V search(K key);

    public abstract byte[] serialize();
    
    public abstract void deserialize(byte[] data);
}