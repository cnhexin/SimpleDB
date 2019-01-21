package simpledb;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    static AtomicLong counter = new AtomicLong(0);
    final long myid;
    
    // PA5 added. Use to track the number of times a transaction failed to acquire a lock
    int timeRequested = 0;
    
    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object tid) {
    	if (!(tid instanceof TransactionId)) {
    		return false;
    	} else {
            return ((TransactionId) tid).myid == myid;
    	}
    }
    
    public String toString() {
    	return String.valueOf(myid);
    }

    public int hashCode() {
        return (int) myid;
    }
}
