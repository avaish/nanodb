package edu.caltech.nanodb.commands;


/**
 * This class represents a command that commits a transaction, such as
 * <tt>COMMIT</tt> or <tt>COMMIT WORK</tt>.
 */
public class CommitTransactionCommand extends Command {
    public CommitTransactionCommand() {
        super(Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        // TODO:  Commit the transaction.
    }
}
