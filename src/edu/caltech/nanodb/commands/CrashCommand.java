package edu.caltech.nanodb.commands;


/**
 * This command "crashes" the database by shutting it down immediately without
 * any proper cleanup or flushing of caches.
 */
public class CrashCommand extends Command {
    /**
     * Construct a new <tt>CRASH</tt> command.
     */
    public CrashCommand() {
        super(Command.Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        out.println("Goodbye, cruel world!  I'm taking your data with me!!!");

        // TODO:  At the point we have a NanoDB shutdown hook, would need to unregister it here.

        System.exit(22);
    }


    /**
     * Prints a simple representation of the crash command.
     *
     * @return a string representing this crash command
     */
    @Override
    public String toString() {
        return "Crash";
    }    
}
