package simpledb;

import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;
import simpledb.transaction.TransactionId;

public class HeapFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = hf.getId();

        System.out.println(id);

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, hf.getId());
        assertEquals(id, hf.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() {
        assertEquals(td, hf.getTupleDesc());
        System.out.println(hf.getTupleDesc());
    }
    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    /**
     * Unit test for HeapFile.readPage()
     */
    @Test
    public void readPage() {
        HeapPageId pid = new HeapPageId(hf.getId(), 0);
        System.out.println(pid);
        HeapPage page = (HeapPage) hf.readPage(pid);

        // NOTE(ghuo): we try not to dig too deeply into the Page API here; we
        // rely on HeapPageTest for that. perform some basic checks.
        assertEquals(484, page.getNumEmptySlots());
        assertTrue(page.isSlotUsed(1));
        assertFalse(page.isSlotUsed(20));
    }

    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 1000, null,
                null);

        DbFileIterator it = smallFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException ignored) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            count += 1;
        }
        assertEquals(1000, count);
        it.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520,
                null, null);

        DbFileIterator it = twoPageFile.iterator(tid);
        it.open();
        assertTrue(it.hasNext());
        it.close();
        try {
            it.next();
            fail("expected exception");
        } catch (NoSuchElementException ignored) {
        }
        // close twice is harmless
        it.close();
    }

//    @Test
//    public void test() {
//        try {
//            // 创建一个临时文件并写入一些数据
//            File tempFile = File.createTempFile("tempfile", ".txt");
//            tempFile.deleteOnExit();
//            try (FileWriter writer = new FileWriter(tempFile)) {
//                writer.write("Hello, World!\n");
//                writer.write("This is a test file.");
//            }
//
//            // 打开文件并读取数据
//            RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile, "r");
//            byte[] buffer = new byte[1024];
//            int bytesRead = randomAccessFile.read(buffer);
//            if (bytesRead != -1) {
//                System.out.println("Read " + bytesRead + " bytes from the file:");
//                System.out.println(new String(buffer, 0, bytesRead));
//            } else {
//                System.out.println("End of file reached.");
//            }
//
//            // 关闭文件
//            randomAccessFile.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
