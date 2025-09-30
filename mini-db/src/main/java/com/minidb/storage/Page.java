package com.minidb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import com.minidb.storage.PageHeader;
import com.minidb.storage.SlotDirectory;
import com.minidb.storage.RecordStorage;
import com.minidb.storage.SlotEntry;

public class Page {
	private final int PAGE_SIZE = 4096;
	private final int HEADER_SIZE = 16;
	private final int SLOT_ENTRY_SIZE  = 12;
	private final int maxSlots;

	// slotStart = PAGE_SIZE - (slotCount * SLOT_ENTRY_SIZE) Defines the slot moves upwards which is inserted above the records.
	private byte[] pageBuffer;
	private PageHeader header;
	private SlotDirectory slots;
	private boolean dirty;
	private int pinCount;		// How many client are using this page. (Used by BufferPool)
	// Lock pageLock;

	// New Empty Page for allocation
	public Page(int pageId,int maxSlots) {
		this.pageBuffer = new byte[PAGE_SIZE];
		this.maxSlots = maxSlots;
		this.dirty = true;
		this.slots = new SlotDirectory(maxSlots);
		this.header = new PageHeader(pageId, HEADER_SIZE);
	}

	// Load from bytes to read from disk
	public Page(byte[] pageBytes, int maxSlots) { // fromBytes
		this.pageBuffer = pageBytes;
		this.maxSlots = maxSlots;

		// Parse header from first HEADER_SIZE bytes
		ByteBuffer headerBuffer = ByteBuffer.wrap(pageBytes, 0, HEADER_SIZE);
		int pageId = headerBuffer.getInt();
		int numSlots = headerBuffer.getInt();
		int freePtr = headerBuffer.getInt();
		
		header = new PageHeader(pageId, freePtr);
		header.setNumSlots(numSlots);

		// Reconstruct slot directory
		slots = new SlotDirectory(maxSlots);
		for (int i=0; i<maxSlots; i++) {
			int slotOffset = PAGE_SIZE - ((i+1) * SLOT_ENTRY_SIZE);
			ByteBuffer slotBuffer = ByteBuffer.wrap(pageBytes, slotOffset, SLOT_ENTRY_SIZE);
			int recordOffset = slotBuffer.getInt();
			int recordLength = slotBuffer.getInt();
			boolean valid = slotBuffer.get() != 0;

			if (valid) {
				// Manually reconstruct the slot state
				slots.addSlot(i, recordOffset, recordLength);
			}
		}
		this.dirty = false;
	}

	public int insertRecord(byte[] record) {
		if (record == null) {
			throw new IllegalArgumentException("Record cannot be null");
		}
		int required = record.length + SLOT_ENTRY_SIZE;
		int nextSlotPos = PAGE_SIZE - ((header.getNumSlots()+1) * SLOT_ENTRY_SIZE);
		int contiguousFree = nextSlotPos - header.getFreeSpacePtr();

		if (record.length > contiguousFree) { 
			return -1;
		}
		else {
			compact();
			nextSlotPos = PAGE_SIZE - ((header.getNumSlots()+1) * SLOT_ENTRY_SIZE);
			contiguousFree = nextSlotPos - header.getFreeSpacePtr();
			if (record.length > contiguousFree) {
				return -1;
			}
		}
		int slotId = slots.addSlot(header.getFreeSpacePtr(), record.length);
		System.arraycopy(record, 0, pageBuffer, header.getFreeSpacePtr(), record.length);
		header.setFreeSpacePtr(header.getFreeSpacePtr() + record.length);
		header.incrementSlots();
		dirty = true;
		return slotId;
	}

	public byte[] getRecord(int slotId) {
		if (!slots.isValid(slotId)) {
			return null;
		}
		SlotEntry slot = slots.get(slotId);
		
		byte[] record = new byte[slot.getLength()];
		System.arraycopy(pageBuffer, slot.getOffset(), record, 0, slot.getLength());
		return record;
	}


	public boolean deleteRecord(int slotId) {
		if (!slots.isValid(slotId)) {
			return false;
		}
		slots.removeSlot(slotId);
		header.decrementSlots();
		dirty = true;
		return true;

	}

	public boolean updateRecord(int slotId, byte[] newRecord) {
		if (!slots.isValid(slotId)) {
			return false;
		}
		SlotEntry slot = slots.get(slotId);
		int oldLen = slot.getLength();
		int newLen = newRecord.length;
		if (oldLen == newLen) {
			// In-place update if length is the same
			System.arraycopy(newRecord, 0, pageBuffer, slot.getOffset(), newLen);
			dirty = true;
			return true;
		} else if (newLen < oldLen) {
			// New record is smaller, can also be updated in-place.
			// This will leave a small hole of (oldLen - newLen) bytes.
			// Compaction will clean this up later.
			System.arraycopy(newRecord, 0, pageBuffer, slot.getOffset(), newLen);
			slot.setLength(newLen);
			dirty = true;
			return true;
		} else { // newLen > oldLen
			int extraNeeded = newLen - oldLen;
			// Check if the *entire new record* can fit in the contiguous free space.
			// This is simpler than trying to append just the extra bytes.
			int slotDirectoryStart = PAGE_SIZE - (header.getNumSlots() * SLOT_ENTRY_SIZE);
			int contiguousFree = slotDirectoryStart - header.getFreeSpacePtr();

			if (newLen <= contiguousFree) {
				// There's enough space at the end. Invalidate old record space and append new one.
				int newOffset = header.getFreeSpacePtr();
				System.arraycopy(newRecord, 0, pageBuffer, newOffset, newLen);
				header.setFreeSpacePtr(newOffset + newLen);
				slot.setOffset(newOffset);
				slot.setLength(newLen);
			} else {
				// Not enough contiguous space, try compacting the page.
				compact();
				slotDirectoryStart = PAGE_SIZE - (header.getNumSlots() * SLOT_ENTRY_SIZE);
				contiguousFree = slotDirectoryStart - header.getFreeSpacePtr();
				if (newLen > contiguousFree) {
					return false; // Not enough space even after compaction.
				}
				// Now there is space. The old record is gone, so we just append the new one.
				// The slot offset was already updated by compact().
				System.arraycopy(newRecord, 0, pageBuffer, slot.getOffset(), newLen);
			}
			dirty = true;
			return true;
		}
	}

	public byte[] toBytes() {
		// The record data is already in pageBuffer, we just need to serialize
		// the header and slot directory into it.

		// 1. Serialize Header
		ByteBuffer buffer = ByteBuffer.wrap(pageBuffer);
		buffer.putInt(0, header.getPageId());
		buffer.putInt(4, header.getNumSlots());
		buffer.putInt(8, header.getFreeSpacePtr());

		// 2. Serialize Slot Directory
		for (int i = 0; i < maxSlots; i++) {
			int pos = PAGE_SIZE - ((i + 1) * SLOT_ENTRY_SIZE);
			SlotEntry slot = slots.get(i);
			buffer.putInt(pos, slot.getOffset());
			buffer.putInt(pos + 4, slot.getLength());
			buffer.put(pos + 8, (byte) (slot.isValid() ? 1 : 0));
		}

		return pageBuffer;
	}

	// Repack all valid records contiguously starting at HEADER_SIZE, update slot offsets, adn set freeSpacePtr
	public void compact() {
		int writePtr = HEADER_SIZE;
		// Create a temporary buffer to avoid overwriting data we still need to read
		byte[] tempBuffer = new byte[PAGE_SIZE];

		// Iterate through all possible slots
		for (int slotIndex = 0; slotIndex < maxSlots; slotIndex++) {
			SlotEntry slot = slots.get(slotIndex);
			if (slot.isValid()) {
				// Copy the record from its old location in pageBuffer to its new location in tempBuffer
				System.arraycopy(pageBuffer, slot.getOffset(), tempBuffer, writePtr, slot.getLength());
				// Update the slot to point to the new offset
				slot.setOffset(writePtr);
				slots.updateSlot(slotIndex, slot.getOffset(), slot.getLength());
				writePtr += slot.getLength();
			}
		}
		header.setFreeSpacePtr(writePtr);
		// Copy the compacted data from tempBuffer back to the main pageBuffer
		System.arraycopy(tempBuffer, 0, pageBuffer, 0, writePtr);
		dirty = true;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void markDirty() {
		dirty = true;
	}
}
