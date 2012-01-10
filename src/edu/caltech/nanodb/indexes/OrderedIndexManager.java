package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.expressions.OrderByExpression;

import java.io.IOException;
import java.util.List;


/**
 * This interface specifies the operations that ordered indexes generally
 * provide.  If an index doesn't provide a particular
 */
public interface OrderedIndexManager extends IndexManager {

    /**
     * Returns the column(s) that are used to order the records in this ordered
     * index.
     *
     * @return the column(s) that are used to order the records in this ordered
     *         index.
     */
    List<OrderByExpression> getOrderSpec();


    // TODO:  Need a more sophisticated specification of what tuple(s) to retrieve.
    IndexPointer findFirstTupleAtLeast(Object[] values) throws IOException;


    // TODO:  Need a more sophisticated specification of what tuple(s) to retrieve.
    IndexPointer rfindFirstTupleAtMost(Object[] values) throws IOException;


    IndexPointer getFirstTuple() throws IOException;


    IndexPointer findNextTuple(IndexPointer entry) throws IOException;


    IndexPointer findPrevTuple(IndexPointer entry) throws IOException;


    IndexPointer getLastTuple() throws IOException;
}
