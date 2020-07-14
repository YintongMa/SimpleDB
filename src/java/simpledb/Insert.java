package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId t;
    OpIterator child;
    OpIterator[] children;
    int tableId;
    boolean ifIssue = false;
    TupleDesc tupleDesc;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.children = new OpIterator[]{child};
        this.tupleDesc = Utility.getTupleDesc(1);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.ifIssue = false;
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.ifIssue = false;
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(ifIssue){
            return null;
        }
        int cnt = 0;
        while (child.hasNext()){
            try {
                Tuple tuple = child.next();
                //System.out.println("fetchNext ###: "+tuple);
//                int v0 = ((IntField)tuple.getField(0)).getValue();
//                int v1 = ((IntField)tuple.getField(1)).getValue();
                //System.out.println("v0: "+v0+",v1: "+v1+",");
                Database.getBufferPool().insertTuple(t,tableId,tuple);
                cnt ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ifIssue = true;
        return new Tuple(Utility.getTupleDesc(1)).setField(0,new IntField(cnt));
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }
}
