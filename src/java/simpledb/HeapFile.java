package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    TupleDesc td;
    File f;
    int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
       //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // create new file input stream
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            // read bytes to the buffer
            //System.out.println("readPage offset:"+pid.getPageNumber()*pageSize+","+pageSize);
            raf.seek(pid.getPageNumber()*pageSize);
            if (raf.read(data) == -1) {
                throw new IllegalArgumentException();
            }
            //raf.read(data, pid.getPageNumber()*BufferPool.getPageSize(), BufferPool.getPageSize());
            raf.close();
            return new HeapPage((HeapPageId) pid,data);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("readPage :"+pid.getPageNumber()+","+pid.getTableId()+","+numPages()+","+f.length());
            throw new IllegalArgumentException(e.getMessage());
        }



    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] data = page.getPageData();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            // read bytes to the buffer
            //System.out.println("readPage offset:"+pid.getPageNumber()*pageSize+","+pageSize);
            raf.seek(page.getId().getPageNumber()*pageSize);
            raf.write(data);
            //raf.read(data, pid.getPageNumber()*BufferPool.getPageSize(), BufferPool.getPageSize());
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();
        //System.out.println("insertTuple: "+numPages()+","+t.getRecordId().getPageId().getPageNumber()+","+f.length());
        for(int i=0;i<numPages();i++){
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),null);
            if(page.getNumEmptySlots()!=0){
                page.insertTuple(t);
                page.markDirty(true,tid);
                modifiedPages.add(page);
                return modifiedPages;
            }

        }

        HeapPageId pageId = new HeapPageId(getId(),numPages());
        writePage(new HeapPage(pageId,new byte[BufferPool.getPageSize()]));
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pageId,null);
        page.insertTuple(t);
        page.markDirty(true,tid);
        modifiedPages.add(page);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),t.getRecordId().getPageId().getPageNumber()),null);
        page.deleteTuple(t);
        page.markDirty(true,tid);
        modifiedPages.add(page);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,f,getId());
    }

    class HeapFileIterator implements DbFileIterator{

        FileInputStream fis;
        TransactionId tid;
        int nextPgNo;
        HeapPage heapPage;
        Iterator<Tuple> tupleIterator;
        File f;
        int tableId;
        boolean hasNext = false;
        boolean isOpen = false;

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        public HeapFileIterator(TransactionId tid,File f,int tableId){
            this.tid = tid;
            this.f = f;
            this.tableId = tableId;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            try {
                isOpen = true;
                nextPgNo = 0;
                fis = new FileInputStream(f);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {

            if(!isOpen){
                return false;
            }

            if(tupleIterator != null){
                if(tupleIterator.hasNext()){
                    hasNext = true;
                    return true;
                }else {
                    nextPgNo++;
                }
            }
            //System.out.println("hasNext :"+"??"+nextPgNo+","+numPages());
            while (nextPgNo < numPages()){
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId,nextPgNo), null);
                tupleIterator = heapPage.iterator();
                if(tupleIterator.hasNext()){
                    hasNext = true;
                    return true;
                }
                nextPgNo++;
            }
            hasNext = false;

            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext || !isOpen){
               throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            try {
                if(fis != null){
                    fis.close();
                }
                heapPage = null;
                tupleIterator = null;
                hasNext = false;
                isOpen = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}

