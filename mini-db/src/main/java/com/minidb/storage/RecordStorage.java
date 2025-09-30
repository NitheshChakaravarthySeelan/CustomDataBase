package com.minidb.storage;
public interface RecordStorage {
	
	// Inserts the record value into the Storage
	int insert(byte[] record);

	// Gets the id of the record
	byte[] get(int slotId);

	// Delete a slot/record
	boolean delete(int slotId);

	// Update a slot/record
	boolean update(int slotId, byte[] newRecord);
}

