package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    LockManager lockManager;

    Map<PageId,Page> pageIdToPage;

    //grain granularity lock
    //public ReentrantReadWriteLock poolLock = new ReentrantReadWriteLock();
    int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lockManager = new LockManager();
        this.pageIdToPage = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
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
        // some code goes here
        LockManager.PageLock pageLock = lockManager.acquireLock(tid,pid,perm);

        Page page;
        if(pageIdToPage.containsKey(pid)){
            page = pageIdToPage.get(pid);
            if(pageLock.perm.equals(Permissions.READ_WRITE)){
                page.setBeforeImage();
            }
            return page;
        }else{
            if(pageIdToPage.size() == numPages){

                lockManager.xlock.lock();
                evictPage();
                lockManager.xlock.unlock();
                //throw new DbException("BufferPool is out of space");
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);
            page.markDirty(false,tid);
            //System.out.println("pageIdToPage.put:"+pid+","+page);
            pageIdToPage.put(pid, page);
            if(pageLock.perm.equals(Permissions.READ_WRITE)){
                page.setBeforeImage();
            }
            return page;
        }
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
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        flushPages(tid);
        lockManager.xlock.lock();
        Set<LockManager.PageLock> pageLocks = lockManager.tidToPageLocks.get(tid);
        for(LockManager.PageLock pageLock : pageLocks){
            pageLock.unlock();
            lockManager.pageLockToTids.get(pageLock).remove(tid);
            if(lockManager.pageLockToTids.get(pageLock).size() == 0){
                lockManager.pageLockToTids.remove(pageLock);
            }
        }
        lockManager.tidToPageLocks.remove(tid);
        lockManager.xlock.unlock();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid,p);
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
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            transactionComplete(tid);
        }else {
            lockManager.xlock.lock();
            Set<LockManager.PageLock> pageLocks = lockManager.tidToPageLocks.get(tid);
            for(LockManager.PageLock pageLock : pageLocks){
                if(pageLock.perm.equals(Permissions.READ_WRITE)){
                    Page page = pageIdToPage.get(pageLock.pageId).getBeforeImage();
                    pageIdToPage.put(pageLock.pageId,page);
                }
                pageLock.unlock();
                lockManager.pageLockToTids.get(pageLock).remove(tid);
                if(lockManager.pageLockToTids.get(pageLock).size() == 0){
                    lockManager.pageLockToTids.remove(pageLock);
                }
            }
            lockManager.tidToPageLocks.remove(tid);
            lockManager.xlock.unlock();
        }
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
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid,t);
        for(Page page:modifiedPages){
            if(pageIdToPage.size() == numPages){
                throw new DbException("BufferPool is out of space");
            }else {
                //System.out.println("bufferpool insertTuple :"+page.getId()+","+pageIdToPage.size());
                pageIdToPage.put(page.getId(),page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    //TODO add locking strategy
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid,t);

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId : pageIdToPage.keySet()){
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if(pid != null){
            pageIdToPage.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageIdToPage.get(pid));
        pageIdToPage.get(pid).markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Map.Entry<PageId,Page> entry:pageIdToPage.entrySet()){
            if(entry.getValue().isDirty() == tid){
                flushPage(entry.getKey());
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //TODO LRU or some cache strategy
        PageId pageId = null;
        for(Map.Entry<PageId,Page> entry : pageIdToPage.entrySet()){
            //System.out.println("evictPage pageIdToPage isDirty:"+entry.getValue().isDirty());
            pageId = entry.getKey();
            if(!lockManager.holdsPotentialLock(pageId) && entry.getValue().isDirty() != null){
                //System.out.println("evictPage pageIdToPage isDirty:"+entry);
                try {
                    flushPage(pageId);
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                break;
            }
        }
        if(pageId != null){
            pageIdToPage.remove(pageId);
            //System.out.println("evictPage pageIdToPage size:"+pageIdToPage.size());
        }

    }

}
