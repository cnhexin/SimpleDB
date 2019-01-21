package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator childIterator;
    private TransactionId tid;
    private TupleDesc resultTD;
    private int tableId;
    private ArrayList<Tuple> result;
    private Iterator<Tuple>	resultIterator;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        tid = t;
        childIterator = child;
        tableId = tableid;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
        	throw new DbException("TupleDesc of the child iterator differs from that of the table which we are about to insert into!");
        }
        resultTD = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"numTups inserted"});
    }

    public TupleDesc getTupleDesc() {
        return resultTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        childIterator.open();
        int count = 0;
        while (childIterator.hasNext()) {
    		Tuple toInsert = childIterator.next();
    		try {
    			Database.getBufferPool().insertTuple(tid, tableId, toInsert);
    			count++;
    		} catch (IOException e) {
    			throw new DbException("Failed to insert tuple: " + toInsert);
    		}
        }
        Tuple resultTuple = new Tuple(resultTD);
        resultTuple.setField(0, new IntField(count));
        result = new ArrayList<Tuple>();
        result.add(resultTuple);
        resultIterator = result.iterator();
    }

    public void close() {
        childIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	childIterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
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
        if (resultIterator.hasNext()) {
        	return resultIterator.next();
        } else {
        	return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { childIterator };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        childIterator = children[0];
        result = null;
        resultIterator = null;
    }
}
