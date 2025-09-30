package com.minidb.storage;
/**
 * SlotDirectory is a metadata structure within a Page that manages the mapping
 * between logical record identifiers (slot IDs) and their physical positions
 * (offsets and lengths) inside the page's byte array.
 *
 * 
 * In a slotted page layout, user records are stored compactly in the "record area"
 * starting from the end of the page and growing downward. The SlotDirectory grows
 * upward from the start of the page, and each slot entry stores:
 * 
 *   offset - the byte position where the record starts
 *   length - the length of the record
 *   validity flag - whether the slot points to an active or deleted record
 *
 * 
 * This indirection allows records to be moved inside the page (e.g., during
 * compaction) without changing their logical record IDs. The SlotDirectory is
 * central for implementing record-level operations such as insert, update, delete,
 * and fetch.
 *
 * Responsibilities
 *   Allocate new slots when inserting records.
 *   Retrieve record metadata (offset, length) given a slot ID.
 *   Mark slots as deleted when records are removed.
 *   Update slot entries if a record is moved or resized.
 *   Provide information about free/invalid slots for reuse.
 * 
 * Relation to Other Components
 *   {@link Page}: Owns the SlotDirectory and raw byte storage.
 *   {@link RecordSerializer}: Converts logical records into byte[] that are placed
 *       into the record area managed by the SlotDirectory.
 *   {@link BufferPool}: Caches Pages containing SlotDirectories.
 *
 * Key Invariants
 *   SlotDirectory entries must not overlap in memory with the record area.
 *   Each active slot must have a valid offset and length within the page boundary.
 *   Deleted slots may be reused but should not return data until reallocated.
 */
public class SlotDirectory {
	// To store record offset
	private int[] slots;
	private boolean[] valid;
	// To store the length
	private int[] lengths;

	public SlotDirectory(int maxSlots) {
		slots = new int[maxSlots];
		valid = new boolean[maxSlots];
		lengths = new int[maxSlots];
	}
	
	public SlotEntry get(int slotId) {
		return new SlotEntry(slots[slotId], lengths[slotId], valid[slotId]);
	}

	public int addSlot(int offset, int length) {
		for (int i=0; i<valid.length; i++) {
			if (!valid[i]) {
				slots[i] = offset;
				lengths[i] = length;
				valid[i] = true;
				// i is the SlotId
				return i;
			}
		}
		return -1;
	}

	public void removeSlot(int slotId) {
		valid[slotId] = false;
		lengths[slotId] = -1;
		slots[slotId] = -1;
	}

	public int getOffset(int slotId) {
		return slots[slotId];
	}

	public int getLength(int slotId) {
		return lengths[slotId];
	}

	public boolean isValid(int slotId) {
		return valid[slotId];
	}


 	public void updateSlot(int slotId, int offset, int length) {
		slots[slotId] = offset;
		lengths[slotId] = length;
	}
 
  	public void addSlot(int slotId, int offset, int length) {
		slots[slotId] = offset;
		lengths[slotId] = length;
		valid[slotId] = true;
	}

}

