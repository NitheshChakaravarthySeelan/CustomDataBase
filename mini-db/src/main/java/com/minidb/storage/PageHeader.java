package com.minidb.storage;

public class PageHeader {

    private final int pageId;
    private int numSlots;
    private int freeSpacePtr;

    public PageHeader(int pageId, int freeSpacePtr) {
        this.pageId = pageId;
        this.freeSpacePtr = freeSpacePtr;
        this.numSlots = 0;
    }

    public int getPageId() {
        return pageId;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public void setNumSlots(int numSlots) {
        this.numSlots = numSlots;
    }

    public int getFreeSpacePtr() {
        return freeSpacePtr;
    }

    public void setFreeSpacePtr(int freeSpacePtr) {
        this.freeSpacePtr = freeSpacePtr;
    }

    public void incrementSlots() {
        numSlots++;
    }

    public void decrementSlots() {
        numSlots--;
    }
}