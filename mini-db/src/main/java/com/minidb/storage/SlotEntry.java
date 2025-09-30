package com.minidb.storage;

public class SlotEntry {
    private int offset;
    private int length;
    private boolean valid;

    public SlotEntry(int offset, int length, boolean valid) {
        this.offset = offset;
        this.length = length;
        this.valid = valid;
    }

    public int getOffset() { return offset; }
    public void setOffset(int offset) { this.offset = offset; }

    public int getLength() { return length; }
    public void setLength(int length) { this.length = length; }

    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }
}