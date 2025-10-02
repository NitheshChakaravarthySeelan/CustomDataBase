package com.minidb.index;

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

    public V search(K key) {
        return root.search(key);
    }

    public void rangeSearch(K startKey, K endKey) {
        Node<K, V> node = root;
        while (!node.isLeaf()) {
            InternalNode<K, V> internalNode = (InternalNode<K, V>) node;
            int pos = internalNode.findChildPosition(startKey);
            node = internalNode.children.get(pos);
        } 
        LeafNode<K, V> leafNode = (LeafNode<K, V>) node;
        while (leafNode != null){
            for (int i = 0; i < leafNode.keys.size(); i++) {
                if (leafNode.keys.get(i).compareTo(startKey) >= 0 && leafNode.keys.get(i).compareTo(endKey) <= 0) {
                    System.out.println("Key: " + leafNode.keys.get(i) + ", Value: " + leafNode.values.get(i));
                }
                if (leafNode.keys.get(i).compareTo(endKey) > 0) {
                    break;
                }
            }
            leafNode = leafNode.nextNode;
        }     
    }

}