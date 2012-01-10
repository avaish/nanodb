package edu.caltech.nanodb.commands;


/**
 * This class represents a command that rolls back a transaction, such as
 * <tt>ROLLBACK</tt> or <tt>ROLLBACK WORK</tt>.
 */
public class RollbackTransactionCommand extends Command {
    public RollbackTransactionCommand() {
        super(Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        // TODO:  Roll back the transaction.
    }
}
