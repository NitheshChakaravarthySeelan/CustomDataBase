package com.minidb.storage;

public class RecordId {
    private final int pageId;
    private final int slotId;

    public RecordId(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public int getPageId() {
        return pageId;
    }

    public int getSlotId() {
        return slotId;
    }
}
