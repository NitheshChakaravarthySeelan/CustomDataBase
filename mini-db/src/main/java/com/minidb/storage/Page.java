import com.minidb.storage.PageHeader;
import com.minidb.storage.SlotDirectory;
import com.minidb.storage.RecordStorage;

public class Page {
	private PageHeader header;
	private SlotDirectory slotDirectory;
	private byte[] data;
	private int pageSize;
	private boolean dirty;
	private int pinCount;
	// private ReentrantLock pageLock;

	public Page(
}
