package edu.caltech.nanodb.commands;


/**
 * This Command class represents the <tt>EXIT</tt> or <tt>QUIT</tt> SQL
 * commands.  These commands aren't standard SQL of course, but are the
 * way that we tell the database to stop.
 */
public class ExitCommand extends Command {

    /** Construct an exit command. */
    public ExitCommand() {
        super(Command.Type.UTILITY);
    }

    /**
     * This method really doesn't do anything, and it isn't intended to be
     * called.
     */
    public void execute() throws ExecutionException {
        // Do nothing.
    }
}
