package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;

import java.io.IOException;

/**
 * This command flushes all unwritten data from the buffer manager to disk.
 * A sync is not performed.
 */
public class FlushCommand extends Command {
    /**
     * Construct a new <tt>CRASH</tt> command.
     */
    public FlushCommand() {
        super(Command.Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        out.println("Flushing all unwritten data to disk.");
        try {
            StorageManager.getInstance().flushAllData();
        }
        catch (IOException e) {
            throw new ExecutionException("IO error during flush!", e);
        }
    }


    /**
     * Prints a simple representation of the flush command.
     *
     * @return a string representing this flush command
     */
    @Override
    public String toString() {
        return "Flush";
    }
}
