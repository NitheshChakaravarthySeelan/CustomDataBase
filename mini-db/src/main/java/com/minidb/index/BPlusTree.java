package com.minidb.index;

import com.minidb.storage.BufferPool;
import com.minidb.storage.Page;
import com.minidb.storage.PageManager;
import com.minidb.storage.RecordId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BPlusTree<K extends Comparable<K>, V> {
    private Node<K, V> root;
    private final int order; // Maximum number of keys in a node
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final PageManager pageManager;
    private final BufferPool bufferPool;
    private final NodeFactory<K, V> nodeFactory;

    public BPlusTree(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer, PageManager pageManager, BufferPool bufferPool) {
        this.order = order;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.pageManager = pageManager;
        this.bufferPool = bufferPool;
        this.nodeFactory = new NodeFactory<>(keySerializer, valueSerializer, order, bufferPool);

        // Try to load the root node from page 1
        try {
            Page page = bufferPool.getPage(1);
            byte[] data = page.toBytes();
            
            // Check if the page is empty (all zeros) or contains valid data
            boolean isEmptyPage = true;
            for (byte b : data) {
                if (b != 0) {
                    isEmptyPage = false;
                    break;
                }
            }

            if (!isEmptyPage) {
                // Determine node type and deserialize
                byte nodeType = data[0];
                if (nodeType == 0) { // InternalNode
                    this.root = nodeFactory.createInternalNode();
                } else { // LeafNode
                    this.root = nodeFactory.createLeafNode();
                }
                this.root.pageId = 1;
                this.root.deserialize(data);
                bufferPool.unpinPage(1, false);
            } else {
                // No root page found, create a new LeafNode as root
                this.root = nodeFactory.createLeafNode();
                this.root.pageId = 1;
                this.root.writeNode(); // Persist the new root
            }
        } catch (IOException e) {
            // Handle error, perhaps create a new root if loading fails
            System.err.println("Error loading BPlusTree root: " + e.getMessage());
            this.root = nodeFactory.createLeafNode();
            this.root.pageId = 1;
            try {
                this.root.writeNode();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        System.out.println("BPlusTree constructor: Root initialized as: " + root.getClass().getSimpleName());
    }

    public void insert(K key, V value) throws IOException {
        System.out.println("BPlusTree.insert: Root hashcode: " + root.hashCode());
        System.out.println("BPlusTree.insert: Root isLeaf(): " + root.isLeaf());
        System.out.println("BPlusTree.insert: Inserting key: " + key + ", value: " + value);
        lock.writeLock().lock();
        try {
        System.out.println("BPlusTree.insert: Root hashcode: " + root.hashCode());
        System.out.println("BPlusTree.insert: Root hashcode: " + root.hashCode());
            LeafNode<K, V> tempLeaf = nodeFactory.createLeafNode();
            Node.SplitResult<K, ? extends Node<K, V>> result = tempLeaf.insert(key, value);
            root.writeNode(); // Write the (potentially modified) root node

            if (result != null) {
                InternalNode<K, V> newRoot = nodeFactory.createInternalNode();
                newRoot.keys.add(result.getSplitKey());
                newRoot.children.add(root);
                newRoot.children.add(result.getRightNode());
                root = newRoot;
                root.writeNode(); // Write the new root node
                result.getRightNode().writeNode(); // Write the new right node
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    

        public void delete(K key) throws IOException {

            lock.writeLock().lock();

            try {

                root.delete(key);

                root.writeNode(); // Write the (potentially modified) root node

    

                if (root instanceof InternalNode && root.keyCount() == 0) {

                    root = ((InternalNode<K, V>) root).children.get(0);

                    root.writeNode(); // Write the new root node

                }

            } finally {

                lock.writeLock().unlock();

            }

        }

    

        public V search(K key) throws IOException {

            lock.readLock().lock();

            try {

                return root.search(key);

            } finally {

                lock.readLock().unlock();

            }

        }

    public List<Map.Entry<K, V>> rangeSearch(K startKey, K endKey) {
        lock.readLock().lock();
        try {
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
        } finally {
            lock.readLock().unlock();
        }
    }

}