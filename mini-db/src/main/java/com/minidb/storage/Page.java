package com.minidb.storage;

import java.nio.ByteBuffer;

public class Page {
    private final int PAGE_SIZE = 4096;
    final static int HEADER_SIZE = 16;
    final static int SLOT_ENTRY_SIZE = 12;
    private final int maxSlots;

    private byte[] pageBuffer;
    private PageHeader header;
    private SlotDirectory slots;
    private boolean dirty;
    private int pinCount;

    public Page(int pageId, int maxSlots) {
        this.pageBuffer = new byte[PAGE_SIZE];
        this.maxSlots = maxSlots;
        this.dirty = true;
        this.pinCount = 1; // Pinned on creation
        this.slots = new SlotDirectory(maxSlots);
        this.header = new PageHeader(pageId, HEADER_SIZE);
        // Initialize the page buffer with header and empty slots
        toBytes(); 
    }

    public Page(byte[] pageBytes, int maxSlots) {
        this.pageBuffer = pageBytes;
        this.maxSlots = maxSlots;
        this.pinCount = 1; // Pinned on load

        ByteBuffer headerBuffer = ByteBuffer.wrap(pageBytes, 0, HEADER_SIZE);
        int pageId = headerBuffer.getInt();
        int numSlots = headerBuffer.getInt();
        int freePtr = headerBuffer.getInt();

        header = new PageHeader(pageId, freePtr);
        header.setNumSlots(numSlots);

        slots = new SlotDirectory(maxSlots);
        for (int i = 0; i < numSlots; i++) {
            int slotOffset = PAGE_SIZE - ((i + 1) * SLOT_ENTRY_SIZE);
            ByteBuffer slotBuffer = ByteBuffer.wrap(pageBytes, slotOffset, SLOT_ENTRY_SIZE);
            int recordOffset = slotBuffer.getInt();
            int recordLength = slotBuffer.getInt();
            boolean valid = slotBuffer.get() != 0;

            if (valid) {
                slots.addSlot(i, recordOffset, recordLength);
            }
        }
        this.dirty = false;
    }

    public int getPageId() {
        return header.getPageId();
    }

    public int insertRecord(byte[] record) {
        if (record == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }
        int requiredSpace = record.length;
        int availableSpace = (PAGE_SIZE - (header.getNumSlots() * SLOT_ENTRY_SIZE)) - header.getFreeSpacePtr();

        if (requiredSpace > availableSpace) {
            compact();
            availableSpace = (PAGE_SIZE - (header.getNumSlots() * SLOT_ENTRY_SIZE)) - header.getFreeSpacePtr();
            if (requiredSpace > availableSpace) {
                return -1; // Not enough space
            }
        }

        int offset = header.getFreeSpacePtr();
        System.arraycopy(record, 0, pageBuffer, offset, record.length);
        header.setFreeSpacePtr(offset + record.length);

        int slotId = slots.addSlot(offset, record.length);
        if (slotId == -1) return -1; // Should not happen if space check is correct

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
        // We don't decrement numSlots from header to keep slotId stable
        // Compaction will physically remove the data
        dirty = true;
        return true;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(pageBuffer);
        buffer.putInt(0, header.getPageId());
        buffer.putInt(4, header.getNumSlots());
        buffer.putInt(8, header.getFreeSpacePtr());

        for (int i = 0; i < maxSlots; i++) {
            int pos = PAGE_SIZE - ((i + 1) * SLOT_ENTRY_SIZE);
            SlotEntry slot = slots.get(i);
            if (slot != null && slot.isValid()) {
                buffer.putInt(pos, slot.getOffset());
                buffer.putInt(pos + 4, slot.getLength());
                buffer.put(pos + 8, (byte) 1);
            } else {
                buffer.putInt(pos, 0);
                buffer.putInt(pos + 4, 0);
                buffer.put(pos + 8, (byte) 0);
            }
        }
        return pageBuffer;
    }

    public void compact() {
        int writePtr = HEADER_SIZE;
        byte[] tempBuffer = new byte[PAGE_SIZE];
        int currentNumSlots = header.getNumSlots();
        int newNumSlots = 0;

        for (int i = 0; i < currentNumSlots; i++) {
            SlotEntry slot = slots.get(i);
            if (slot != null && slot.isValid()) {
                byte[] record = getRecord(i);
                System.arraycopy(record, 0, tempBuffer, writePtr, record.length);
                slots.updateSlot(i, writePtr, record.length);
                writePtr += record.length;
                newNumSlots++;
            }
        }
        System.arraycopy(tempBuffer, 0, pageBuffer, 0, writePtr);
        header.setFreeSpacePtr(writePtr);
        header.setNumSlots(newNumSlots);
        dirty = true;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public int getPinCount() {
        return pinCount;
    }

    public void pin() {
        pinCount++;
    }

    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }
}
