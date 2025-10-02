package com.minidb.storage;

public class SlotEntry {

    private int offset;
    private int length;
    private boolean valid;

    public SlotEntry() {
        this.valid = false;
    }

    public void set(int offset, int length, boolean valid) {
        this.offset = offset;
        this.length = length;
        this.valid = valid;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void setOffset(int writePtr) {
        this.offset = writePtr;
    }

    public void setLength(int newLen) {
        this.length = newLen;
    }
}
