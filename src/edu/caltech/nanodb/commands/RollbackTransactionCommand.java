package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionException;
import edu.caltech.nanodb.transactions.TransactionManager;


/**
 * This class represents a command that rolls back a transaction, such as
 * <tt>ROLLBACK</tt> or <tt>ROLLBACK WORK</tt>.
 */
public class RollbackTransactionCommand extends Command {
    public RollbackTransactionCommand() {
        super(Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        // Roll back the transaction.

        TransactionManager txnMgr =
            StorageManager.getInstance().getTransactionManager();

        try {
            txnMgr.rollbackTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
