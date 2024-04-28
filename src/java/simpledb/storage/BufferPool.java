package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final int DEFAULT_PAGE_SIZE = 4096;
    //每一页的大小
    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    //默认buffer中保存的页数
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
//    private ConcurrentHashMap<PageId, Page> buffer;
    private LRUCache<PageId, Page> buffer;
    LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
//        this.buffer = new ConcurrentHashMap<>();
        this.buffer = new LRUCache<>(numPages);
        this.lockManager = new LockManager();
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
    // 未实现页面淘汰策略的getPage
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws TransactionAbortedException, DbException {
//        // some code goes here
//        if(!this.buffer.containsKey(pid)) {
//            if(numPages > buffer.size()) {
//                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//                Page page = dbFile.readPage(pid);
//                if(page != null) {
//                    buffer.put(pid, page);
//                }
//                else return null;
//            }
//            else {
//                throw new DbException("bufferPool is full");
//            }
//        }
//        return this.buffer.get(pid);
//    }

    // 实现页面淘汰策略的getPage
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws TransactionAbortedException, DbException {
//        // some code goes here
//        if(this.buffer.get(pid) == null) {
//            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = dbFile.readPage(pid);
//            // page == null ?
//            if(page == null) throw new DbException("page null");
//            if(buffer.getSize() >= numPages) evictPage();
//            buffer.put(pid, page);
//            return page;
//        }
//        return this.buffer.get(pid);
//    }

    // 实现页面淘汰策略 + 页级的锁管理器 的getPage
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //先获取锁
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000);
        while(!lockAcquired){
            long now = System.currentTimeMillis();
            if(now - start> timeout){
                // 获取锁超时抛出异常之前是否需要释放该事务已经有用的锁
                throw new TransactionAbortedException();
            }
            lockAcquired = lockManager.acquireLock(tid,pid,perm);
        }

        if(this.buffer.get(pid) == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if(page == null) throw new DbException("page null");
            if(buffer.getSize() >= numPages) evictPage();
            buffer.put(pid, page);
            return page;
        }
        return this.buffer.get(pid);
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) {
            // 在事务提交时，您应该强制将脏页写入磁盘（例如，将页写出）（这是 FORCE）
            try{
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            rollback(tid);
        }
        lockManager.releaseAllLock(tid);
    }

    private synchronized void rollback(TransactionId tid) {
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head != tail) {
            Page page = head.value;
            LRUCache<PageId, Page>.DLinkedNode next = head.next;
            if(page != null && page.isDirty() != null && page.isDirty().equals(tid)) {
                buffer.remove(head);
                try {
                    Page page1 = Database.getBufferPool().getPage(tid, page.getId(), Permissions.READ_ONLY);
                    page1.markDirty(false, null);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }

            }
            head = next;
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        //注意，insertTuple函数并不会
        List<Page> pages = dbFile.insertTuple(tid, t);
        for(Page page : pages){
            page.markDirty(true,tid);
            // put方法中当脏页被移除时没有触发刷盘
            buffer.put(page.getId(),page);
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid,t);
        for(Page page: pages){
            page.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while (head != tail) {
            Page page = head.value;
            if(page != null && page.isDirty() != null) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try {
//                    page.markDirty(false,null);
                    dbFile.writePage(page);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//            flushPage(page.getId());
            head = head.next;
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
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            PageId key = head.key;
            if(key!=null && key.equals(pid)){
                buffer.remove(head);
                return;
            }
            head = head.next;
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = buffer.get(pid);
        if(page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try {
                page.markDirty(false, null);
                dbFile.writePage(page);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head != tail) {
            Page page = head.value;
            if(page != null && page.isDirty() != null && page.isDirty().equals(tid)) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try {
                    page.markDirty(false, null);
                    dbFile.writePage(page);
                    page.setBeforeImage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page page = buffer.getTail().prev.value;
        if(page != null && page.isDirty() != null) {
            //将脏页写入磁盘
//            try {
//                flushPage(page.getId());
//            }catch (IOException e) {
//                e.printStackTrace();
//            }
            // lab4 ex3 需要选取一个非脏页进行置换
            findNotDirty();
        } else {
            //不是脏页没改过，不需要写磁盘
//            discardPage(page.getId());
            buffer.discard();
        }
    }

    // 从LRU链表中从后往前找到第一个非脏页并移除
    private void findNotDirty() throws DbException {
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        tail = tail.prev;
        while (head != tail) {
            Page value = tail.value;
            if (value != null && value.isDirty() == null) {
                buffer.remove(tail);
                return;
            }
            tail = tail.prev;
        }
        //没有非脏页，抛出异常
        throw new DbException("no dirty page");
    }

}
