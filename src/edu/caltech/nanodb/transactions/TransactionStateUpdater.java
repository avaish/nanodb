package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.commands.BeginTransactionCommand;
import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.commands.CommitTransactionCommand;
import edu.caltech.nanodb.commands.RollbackTransactionCommand;
import edu.caltech.nanodb.server.CommandEventListener;
import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.storage.BufferManager;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;


/**
 * This implementation of the {@link CommandEventListener} interface manages the
 * transaction state enclosing each command executed by the database.  This
 * includes starting a transaction before each command, if one is not already in
 * progress, and
 */
public class TransactionStateUpdater implements CommandEventListener {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(TransactionStateUpdater.class);

    
    private TransactionManager transactionManager;


    private BufferManager bufferManager;
    
    
    public TransactionStateUpdater(TransactionManager transactionManager,
                                   BufferManager bufferManager) {
        this.transactionManager = transactionManager;
        this.bufferManager = bufferManager;
    }
    
    
    @Override
    public void beforeCommandExecuted(Command cmd) throws EventDispatchException {

        if (cmd instanceof BeginTransactionCommand ||
            cmd instanceof CommitTransactionCommand ||
            cmd instanceof RollbackTransactionCommand) {
            // We don't do anything for these kinds of commands, since the
            // commands themselves handle the transaction state.
            return;
        }

        // Check if a new transaction needs to be started.
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();
        logger.debug("Session ID:  " + state.getSessionID() +
            "\tTransaction state:  " + txnState);

        if (!txnState.isTxnInProgress()) {
            // Start one!
            try {
                logger.debug("No transaction is in progress; auto-starting one!");
                transactionManager.startTransaction(false);
            }
            catch (TransactionException e) {
                throw new EventDispatchException(e);
            }
        }
    }


    @Override
    public void afterCommandExecuted(Command cmd) throws EventDispatchException {
        // Check if the transaction needs to be auto-committed.
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();
        logger.debug("Session ID:  " + state.getSessionID() +
            "\tTransaction state:  " + txnState);

        // It's possible that the transaction may have already been committed
        // or aborted, e.g. if a COMMIT or ROLLBACK has been issued, or if a
        // constraint has been violated and the database decided to rollback
        // the transaction.
        if (txnState.isTxnInProgress()) {
            // Only auto-commit the transaction if the user didn't manually
            // start the transaction.
            if (!txnState.getUserStartedTxn()) {
                // Auto-commit the command.
                try {
                    logger.debug("An auto-started transaction is in progress;" +
                        " committing it!");
                    transactionManager.commitTransaction();
                }
                catch (TransactionException e) {
                    throw new EventDispatchException(e);
                }
            }
        }

        // Always unpin all pages that the client has pinned during the command.
        // This way they can be evicted from the cache if necessary.
        bufferManager.unpinAllPages();
    }
}
