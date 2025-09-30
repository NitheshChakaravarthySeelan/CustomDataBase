package com.minidb.storage;

/**
 * pageId
 * 		Identifies the page uniquely
 * 		Lets the DB reference this page from indexes, free-space maps
 * numSlots
 * 		How many records(slots) are currently in this page
 * 		Many databases use a slot directory at the end of the page that points to the actual records
 * 		It tells us how many entries in the slot directory are valid.
 * freeSpacePtr
 * 		Pointer (offset) into the page where the next free space begins
 * 		Lets the DB know where it can write the next record.
 */
public class PageHeader {
	private int pageId;
	private int numSlots;
	private int freeSpacePtr;

	public PageHeader(int pageId, int startPtr) {
		this.pageId = pageId;
		this.freeSpacePtr = startPtr;
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

	public void incrementSlots() {
		numSlots++;
	}

	public void decrementSlots() {
		numSlots--;
	}

	public int getFreeSpacePtr() {
		return freeSpacePtr;
	}

	public void setFreeSpacePtr(int ptr) {
		freeSpacePtr = ptr;
	}
}
