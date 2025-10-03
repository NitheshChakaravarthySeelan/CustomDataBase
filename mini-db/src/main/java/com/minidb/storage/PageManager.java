package com.minidb.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

/** 
 * Create or open a database file
 *      Keep a reference to the file on disk.
 *      Maintain its page size and current number of pages.
 * Allocate new Page
 *      Decide the next available PageID
 *      Initialize a blank page in memory
 *      Optionally write it to disk immediately
 * Read page
 *      Give a pageId, return the page bytes
 *      Read from disk not in memory
 * Write page
 *      Given a pageId and byte[] data, write it to disk 
 *      Ensure atomicity if possible (write temp, rename file or orverwrite directly)
 * Track free pages
 *      Optionally: Maintain a free list 
 *      Could be in memory and persist in a metadata page
 * Close / flush
 *      Flush all pending writes to disk
 *      Release file handles.
 */
public class PageManager {
	private static final String FILE_SIGNATURE = "MINIDB";
	private static final int FILE_FORMAT_VERSION = 1;

	private String dbFilePath;
	private int pageSize;
	private int numPages; // Total number of pages in the database file
	private RandomAccessFile dbFile;
	private Queue<Integer> freePageList;

	public PageManager(String dbFilePath, int pageSize) throws IOException {
		this.dbFilePath = dbFilePath;
		this.pageSize = pageSize;
		File file = new File(dbFilePath);

		if (file.exists() && file.length() > 0) {
			loadExistingDbFile(file);
		} else {
			initializeNewDbFile(file);
		}
	}

	private void initializeNewDbFile(File file) throws IOException {
		this.dbFile = new RandomAccessFile(file, "rw");
		// A new file starts with just the metadata page (Page 0)
		this.freePageList = new LinkedList<>();
		this.numPages = 1;
		
		// Create and write the metadata page
		byte[] metaPage = new byte[pageSize];
		writeMetadata(metaPage);
		writePage(0, metaPage);
	}

	private void loadExistingDbFile(File file) throws IOException {
		this.dbFile = new RandomAccessFile(file, "rw");
		if (dbFile.length() < pageSize) {
			throw new IOException("Database file is corrupted or too small.");
		}

		// Read the first page to get metadata
		byte[] metaPage = new byte[pageSize];
		readPage(0, metaPage);
		readMetadata(metaPage);
		// In a more robust system, the free page list would also be serialized here.
		this.freePageList = new LinkedList<>();
	}

	/**
	 * Writes metadata to the provided byte array (which should be Page 0).
	 */
	private void writeMetadata(byte[] metaPage) {
		ByteBuffer buffer = ByteBuffer.wrap(metaPage);
		buffer.put(FILE_SIGNATURE.getBytes()); // 6 bytes
		buffer.putShort((short) FILE_FORMAT_VERSION); // 2 bytes
		buffer.putInt(this.pageSize); // 4 bytes
		buffer.putInt(this.numPages); // 4 bytes
		// We could serialize the freePageList here as well.
	}

	/**
	 * Reads and validates metadata from the provided byte array (Page 0).
	 */
	private void readMetadata(byte[] metaPage) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(metaPage);
		byte[] signatureBytes = new byte[FILE_SIGNATURE.length()];
		buffer.get(signatureBytes);
		if (!new String(signatureBytes).equals(FILE_SIGNATURE)) {
			throw new IOException("Not a valid MiniDB database file.");
		}

		int version = buffer.getShort();
		if (version != FILE_FORMAT_VERSION) {
			throw new IOException("Unsupported database file version: " + version);
		}

		int filePageSize = buffer.getInt();
		if (filePageSize != this.pageSize) {
			throw new IOException("Incorrect page size. Expected " + this.pageSize + ", but file has " + filePageSize);
		}

		this.numPages = buffer.getInt();
	}

	/**
	 * Writes the given byte array data for a page to the database file.
	 * @param pageId The ID of the page to write.
	 * @param data The byte array of page data.
	 * @throws IOException If an I/O error occurs.
	 */
	public void writePage(int pageId, byte[] data) throws IOException {
        long offset = (long) pageId * pageSize;
        dbFile.seek(offset);
        dbFile.write(data);
    }

	/**
	 * Reads a page from the database file into the provided buffer.
	 * @param pageId The ID of the page to read.
	 * @param buffer The buffer to read the page data into.
	 * @throws IOException If an I/O error occurs.
	 */
	public void readPage(int pageId, byte[] buffer) throws IOException {
        long offset = (long) pageId * pageSize;
        dbFile.seek(offset);
        dbFile.read(buffer);
    }

	/**
	 * Allocates a new page, either by reusing a free page or extending the file.
	 * @return The page ID of the newly allocated page.
	 */
    public int allocatePage() {
		if (!freePageList.isEmpty()) {
			return freePageList.poll();
		} else {
			return numPages++;
		}
	}

	public void freePage(int pageId) {
		freePageList.add(pageId);
	}

	public void close() throws IOException {
		// Flush metadata changes (like numPages) to disk before closing.
		byte[] metaPage = new byte[pageSize];
		// We need to read it first to not clobber other metadata if it existed
		readPage(0, metaPage);
		writeMetadata(metaPage);
		writePage(0, metaPage);
		dbFile.close();
	}

	public int getPageSize() {
		return 4096;
	}
}