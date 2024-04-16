package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableId somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapFile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
//    public Page readPage(PageId pid) {
//        // some code goes here
//        HeapPage heapPage = null;
//        int pageSize = BufferPool.getPageSize();
//        byte[] buf = new byte[pageSize];
//        try {
//            // "随机" 的意思是可以在文件的任意位置进行读取和写入，并不是指按照某种随机的顺序读取文件的内容
//            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
//            randomAccessFile.seek((long)pid.getPageNumber()*pageSize);
//            //如果 read 方法返回 -1，表示文件已经读取到末尾，此时返回 null
//            if(randomAccessFile.read(buf)==-1){
////                return null;
//                return new HeapPage((HeapPageId) pid, Arrays.copyOf(buf, pageSize));
//            }
//            heapPage= new HeapPage((HeapPageId) pid, buf);
//            randomAccessFile.close();
//        } catch (FileNotFoundException e ) {
//            e.printStackTrace();
//        } catch (IOException e){
//            e.printStackTrace();
//        }
//        return heapPage;
////        return null;
//    }

    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            randomAccessFile.seek((long)pid.getPageNumber()*pageSize);
            if(randomAccessFile.read(buf)==-1){
                return null;
            }
            heapPage= new HeapPage((HeapPageId) pid, buf);
            randomAccessFile.close();
        } catch (FileNotFoundException e ) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        return heapPage;
    }

//    public Page readPage(PageId pid) {
//        // some code goes here
//        int tableId = pid.getTableId();
//        int pageNumber = pid.getPageNumber();
//
//        int pageSize = Database.getBufferPool().getPageSize();
//        long offset = pageNumber * pageSize;
//        byte[] data = new byte[pageSize];
//        RandomAccessFile rfile = null;
//        try {
//            rfile = new RandomAccessFile(file, "r");
//            rfile.seek(offset);
////            rfile.read(data);
//            System.out.println("ddata"+rfile.read(data));
//            HeapPageId heapPageId = new HeapPageId(tableId, pageNumber);
//            HeapPage heapPage = new HeapPage(heapPageId, data);
//            return heapPage;
//        } catch (FileNotFoundException e) {
//            throw new IllegalArgumentException("HeapFile: readPage: file not found");
//        } catch (IOException e) {
//            throw new IllegalArgumentException(String.format("HeapFile: readPage: file with offset %d not found",offset));
//        } finally {
//            try {
//                rfile.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public Page readPage(PageId pid) throws IllegalArgumentException {
//        if (!(pid instanceof HeapPageId))
//            throw new IllegalArgumentException("pid should be HeapPageId");
//
//        int size = BufferPool.getPageSize();
//
//        try (RandomAccessFile f = new RandomAccessFile(this.file, "r")) {
//            f.seek((long) size * pid.getPageNumber());
//
//            byte[] data = new byte[size];
//            f.read(data, 0, size);
//
//            return new HeapPage((HeapPageId) pid, data);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, Permissions.READ_ONLY);
    }

    /**
     * 这个迭代器的作用是用来遍历所有的tuple，但是不要将所有tuple一次性放入内存，而是一页一页的读和遍历
     */
    public class  HeapFileIterator implements DbFileIterator {
        TransactionId tid;
        Permissions permissions;
        BufferPool bufferPool = Database.getBufferPool();
        Iterator<Tuple> iterator;
        int num = 0;

        public HeapFileIterator(TransactionId tid,Permissions permissions){
            this.tid = tid;
            this.permissions = permissions;
        }

        /**
         * 开始进行遍历，默认从第一页开始
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);
            HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);
            if(page == null){
                throw new DbException("page null");
            }else{
                iterator = page.iterator();
            }
        }

        /**
         * 获取下一页数据
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public boolean nextPage() throws DbException, TransactionAbortedException {
            while (true) {
                num++;
                if(num > numPages()) return false;
                HeapPageId heapPageId = new HeapPageId(getId(), num);
                HeapPage page = (HeapPage)bufferPool.getPage(tid,heapPageId,permissions);
                if(page == null) continue;
                iterator = page.iterator();
                if(iterator.hasNext()) return true;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(iterator == null){
                return false;
            }
            if(iterator.hasNext()){
                return true;
            }else{
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(iterator == null){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

