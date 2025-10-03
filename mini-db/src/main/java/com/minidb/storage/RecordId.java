package com.minidb.storage;

import java.nio.ByteBuffer;

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

    public byte[] serialize() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putInt(pageId);
        bb.putInt(slotId);
        return bb.array();
    }

    public static RecordId deserialize(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int pageId = bb.getInt();
        int slotId = bb.getInt();
        return new RecordId(pageId, slotId);
    }
}
