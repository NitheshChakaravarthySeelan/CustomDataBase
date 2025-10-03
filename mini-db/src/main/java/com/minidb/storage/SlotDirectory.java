package com.minidb.storage;

public class SlotDirectory {

    private final SlotEntry[] slots;

    public SlotDirectory(int maxSlots) {
        this.slots = new SlotEntry[maxSlots];
        for (int i = 0; i < maxSlots; i++) {
            this.slots[i] = new SlotEntry();
        }
    }

    public int addSlot(int offset, int length) {
        for (int i = 0; i < slots.length; i++) {
            if (!slots[i].isValid()) {
                slots[i].set(offset, length, true);
                return i;
            }
        }
        return -1; // No free slots
    }

    public void removeSlot(int slotId) {
        slots[slotId].setValid(false);
    }

    public boolean isValid(int slotId) {
        return slots[slotId].isValid();
    }

    // This method was missing but referenced in Page.java's logic
    public int findFreeSlot() {
        // This is a simplified version of what addSlot does
        return addSlot(0, 0);
    }
    public SlotEntry get(int slotId) {
        return slots[slotId];
    }

    public void updateSlot(int slotIndex, int offset, int length) {
        slots[slotIndex].set(offset, length, true);
    }

    public void addSlot(int i, int recordOffset, int recordLength) {
        slots[i].set(recordOffset, recordLength, true);
    }
}