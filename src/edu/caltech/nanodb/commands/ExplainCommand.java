package edu.caltech.nanodb.commands;


/**
 * This Command class represents the <tt>EXPLAIN</tt> SQL command, which prints
 * out details of how SQL DML statements will be evaluated.
 */
public class ExplainCommand extends Command {

    /** The command to explain! */
    private QueryCommand cmdToExplain;


    /**
     * Construct an explain command.
     *
     * @param cmdToExplain the command that should be explained.
     */
    public ExplainCommand(QueryCommand cmdToExplain) {
        super(Command.Type.UTILITY);

        this.cmdToExplain = cmdToExplain;
    }


    public void execute() throws ExecutionException {
        cmdToExplain.setExplain(true);
        cmdToExplain.execute();
    }
}
