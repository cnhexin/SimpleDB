package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
	public static final int DEFAULT_PAGES = 50;
	private static int pageSize = PAGE_SIZE;
	// PA5
	final int numPages;   
	final ConcurrentHashMap<PageId,Page> pages; 
	private final Random random = new Random(); 

	private final LockManager lockmgr; 

	// Constructor
	public BufferPool(int numPages) {
		this.numPages = numPages;
		this.pages = new ConcurrentHashMap<PageId, Page>();
		lockmgr = new LockManager(); 
	}

	public static int getPageSize() {
		return pageSize;
	}

	 /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		try {
			lockmgr.acquireLock(tid, pid, perm);
		} catch (DeadlockException e) { 
			throw new TransactionAbortedException(); // caught by callee, who calls transactionComplete()
		}


		Page p;
		synchronized(this) {
			p = pages.get(pid);
			if(p == null) {
				if(pages.size() >= numPages) {

					evictPage(); 
				}

				p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
				pages.put(pid, p);
			}
		}
		return p;
	}

	 /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
	public  void releasePage(TransactionId tid, PageId pid) {   	
		lockmgr.releaseLock(tid,pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		transactionComplete(tid,true); 
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		return lockmgr.holdsLock(tid, p); 
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		lockmgr.releaseAllLocks(tid, commit);
	}
   public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
	/**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {

		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirtypages = file.insertTuple(tid, t);

		synchronized(this) {
			for (Page p : dirtypages){
				p.markDirty(true, tid);
				if(pages.get(p.getId()) != null) {
					pages.put(p.getId(), p);
				}
				else {
					if(pages.size() >= numPages)
						evictPage();
					pages.put(p.getId(), p);
				}
			}
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have 
	 * been dirtied so that future requests see up-to-date pages. 
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t the tuple to delete
	 */
	public  void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirtypages = file.deleteTuple(tid, t);
		synchronized(this) {
			for (Page p : dirtypages){
				p.markDirty(true, tid);
			}
		}
	}

	/**
	 * Flush all dirty pages to disk.
	 * Be careful using this routine -- it writes dirty data to disk so will
	 *     break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		Iterator<PageId> i = pages.keySet().iterator();
		while(i.hasNext())
			flushPage(i.next());
	}

	/** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
	 */
	public synchronized void discardPage(PageId pid) {
		pages.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		Page p = pages.get(pid);
		if (p == null)
			return; 
		if (p.isDirty() == null) {
			return; 
		}
		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
		file.writePage(p);
		p.markDirty(false, null);
	}

	/** Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {

	}

	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 * Throws a DbException if unable to evict a page
	 */
	private synchronized void evictPage() throws DbException {
		// Evict a random page
		Object pids[] = pages.keySet().toArray();
		PageId pid = (PageId) pids[random.nextInt(pids.length)];

		try {
			Page p = pages.get(pid);
			if (p.isDirty() != null) 
			{ //find a non-dirty page
				for (PageId pg : pages.keySet()) 
				{
					if (pages.get(pg).isDirty() == null) 
					{
						pid = pg;
						break;
					}
				}
			}
			if (pages.get(pid).isDirty() == null) 
			{
				flushPage(pid); 
			} else {
				throw new DbException(":( All page dirty. Eviction failed!");
			}
		} catch (IOException e) {
			throw new DbException(":( Error during eviction!");
		}
		pages.remove(pid);
	}
	
	private class Lock {
		final PageId pageLocked;
		final Permissions level;
		
		public Lock(PageId pageId, Permissions perm) {
			this.pageLocked = pageId;
			this.level = perm;
		}
		
		public boolean equals(Object other) {
			if (!(other instanceof Lock)) {
				return false;
			}
			Lock toCompare = (Lock) other;
			if (this.level.equals(toCompare.level) && this.pageLocked.equals(toCompare.pageLocked)) {
				return true;
			}
			return false;
		}
		
		public int hashCode() {
			return (this.pageLocked.toString() + this.level.toString()).hashCode();
		}
	}

	/**
	 * A class for PA5 that manages locks on PageIds held by TransactionIds.
	 * @Threadsafe
	 */
	private class LockManager {
		final int lockWait = 10;       
		final int maxFailture = 10;		
		//keep track of page-level locks for transactions
		final ConcurrentHashMap<Lock, HashSet<TransactionId>> lockTable;
		private LockManager() {
			lockTable = new ConcurrentHashMap<Lock, HashSet<TransactionId>>();
		}
		/**
		 * Acquire a lock on pageid, with permissions perm. 
		 * If this method cannot acquire the lock, given a timeout period, it would wait and then re-try. 
		 * @throws DeadlockException after on cycle-based deadlock
		 */
		public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
				throws DeadlockException {
			while(!lock(tid, pid, perm)) {// before successfully acquired the lock

				synchronized(this) 
				{
					// Added timeRequested variable for PA5.
					tid.timeRequested++;
					if (tid.timeRequested > maxFailture) 
					{
						throw new DeadlockException();
					}
				}
				try 
				{
					Thread.sleep(lockWait); // pended
				} catch (InterruptedException e) {
				}
			}
			synchronized(this) 
			{
				tid.timeRequested = 0;
			}

			return true;
		}
		/**
		 * Release all locks corresponding to TransactionId.
		 */
		public synchronized void releaseAllLocks(TransactionId tid, boolean commit) {
			HashSet<Lock> locksHeld = new HashSet<Lock>();
			for (Lock lock : lockTable.keySet()) {
				if (holdsLock(tid,lock.pageLocked)) {
					locksHeld.add(lock);
				}
			}
			for (Lock lock : locksHeld) {
				if (commit) {
					try {
						flushPage(lock.pageLocked);
					} catch (IOException e) {
						throw new RuntimeException("Couldn't flush page with id: " + lock.pageLocked);
					}
				} else {
					discardPage(lock.pageLocked);
				}
				releaseLock(tid, lock.pageLocked);
			}
		}

		/** Return true if the transaction has a read lock on the specified page */
		private synchronized boolean holdsReadLock(TransactionId tid, PageId p) {
			HashSet<TransactionId> sLocks = lockTable.get(new Lock(p, Permissions.READ_ONLY));
			return (sLocks == null) ? false : sLocks.contains(tid);
		}
		
		/** Return true if the transaction has a write lock on the specified page */
		private synchronized boolean holdsWriteLock(TransactionId tid, PageId p) {
			HashSet<TransactionId> xLocks = lockTable.get(new Lock(p, Permissions.READ_WRITE));
			return (xLocks == null) ? false : xLocks.contains(tid);
		}
		
		/** Return true if the transaction has a lock on the specified page */
		public synchronized boolean holdsLock(TransactionId tid, PageId p) {
			return (holdsReadLock(tid, p) || holdsWriteLock(tid, p));
		}

		private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
			HashSet<TransactionId> blockers = blockedBy(tid,pid,perm);
			return !(blockers.isEmpty());
		}
		
		/**
		 * Returns a set of blocked Transactions
		 */ 
		private synchronized HashSet<TransactionId> blockedBy(TransactionId tid, PageId pid, Permissions perm) {
			HashSet<TransactionId> blockedTrans = new HashSet<TransactionId>();
			if (perm.equals(Permissions.READ_ONLY)) {
				Lock lock = new Lock(pid, Permissions.READ_WRITE);
				if (lockTable.containsKey(lock)) {
					for (TransactionId id : lockTable.get(lock)) {
						if (!id.equals(tid)) {
							blockedTrans.add(id);
						}
					}
				}
				return blockedTrans;
			} else {
				HashSet<TransactionId> readers = lockTable.get(new Lock(pid, Permissions.READ_ONLY));
				HashSet<TransactionId> writers = lockTable.get(new Lock(pid, Permissions.READ_WRITE));
				if (readers != null) {
					for (TransactionId id : readers) {
						if (!id.equals(tid)) {
							blockedTrans.add(id);
						}
					}
				}
				if (writers != null) {
					for (TransactionId id : writers) {
						if (!id.equals(tid)) {
							blockedTrans.add(id);
						}
					}
				}
				return blockedTrans;
			}
		}

		/*
		 * This method would also update lock table
		 */
		public synchronized void releaseLock(TransactionId tid, PageId pid) {
			Lock readOnly = new Lock(pid, Permissions.READ_ONLY);
			HashSet<TransactionId> sTransactions = lockTable.get(readOnly);
			if (sTransactions != null) {
				if (sTransactions.contains(tid)) {
					sTransactions.remove(tid);
					lockTable.put(readOnly, sTransactions);
				}
			}
			Lock readWrite = new Lock(pid, Permissions.READ_WRITE);
			HashSet<TransactionId> xTransactions = lockTable.get(readWrite);
			if (xTransactions != null) {
				if (xTransactions.contains(tid)) {
					lockTable.remove(readWrite);
				}
			}
		}

		/*
		 * Lock the given PageId with the given Permissions for this TransactionId
		 * This method would also update lock table
		 */
		private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
			if(locked(tid, pid, perm)) 
				return false; // this transaction cannot get the lock on this page; it is "locked out"

			Lock toAcquire = new Lock(pid, perm);
			if (lockTable.containsKey(toAcquire)) {
				HashSet<TransactionId> transactions = lockTable.get(toAcquire);
				transactions.add(tid);
				lockTable.put(toAcquire, transactions);
			} else {
				HashSet<TransactionId> transactions = new HashSet<TransactionId>();
				transactions.add(tid);
				lockTable.put(toAcquire, transactions);
			}

			return true;
		}
	}
	
	

}