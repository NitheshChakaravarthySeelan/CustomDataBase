public class PageHeader {
	private int pageId;
	private int numSlots;
	private int freeSpacePtr;

	public PageHeader(int pageId, int startPtr) {
		this.pageId = pageId;
		this.freeSpacePtr = startPtr;
		this.numSlots = 0;
	}

	public int getPageId() {
		return pageId;
	}

	public int getNumSlots() {
		return numSlots;
	}

	public void incrementSlots() {
		numSlots++;
	}

	public void decrementSlots() {
		numSlots--;
	}

	public int getFreeSpacePtr() {
		return freeSpacePtr;
	}

	public void setFreeSpacePtr(int ptr) {
		freeSpacePtr = ptr;
	}
