package com.minidb.storage;

public class PageHeader {

    private final int pageId;
    private int numSlots;
    private int freeSpacePtr;
    private byte pageType; // 0: data, 1: leaf, 2: internal

    public PageHeader(int pageId, int freeSpacePtr) {
        this.pageId = pageId;
        this.freeSpacePtr = freeSpacePtr;
        this.numSlots = 0;
        this.pageType = 0;
    }

    public byte getPageType() {
        return pageType;
    }

    public void setPageType(byte pageType) {
        this.pageType = pageType;
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