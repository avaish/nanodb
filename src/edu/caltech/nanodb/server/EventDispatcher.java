package edu.caltech.nanodb.server;


import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.TableFileInfo;
import org.apache.log4j.Logger;

import java.util.ArrayList;


/**
 */
public class EventDispatcher {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(EventDispatcher.class);


    /** This is the singleton instance of the event-dispatcher. */
    private static EventDispatcher instance = new EventDispatcher();


    /**
     * Returns the singleton instance of the event dispatcher.
     * 
     * @return the singleton instance of the event dispatcher.
     */
    public static EventDispatcher getInstance() {
        return instance;
    }


    private ArrayList<CommandEventListener> commandEventListeners =
        new ArrayList<CommandEventListener>();


    private ArrayList<RowEventListener> rowEventListeners =
        new ArrayList<RowEventListener>();


    public void addCommandEventListener(CommandEventListener listener) {
        commandEventListeners.add(listener);
    }


    /**
     *
     * @param cmd the command that is about to be executed
     */
    public void fireBeforeCommandExecuted(Command cmd) {
        for (CommandEventListener cel : commandEventListeners)
            cel.beforeCommandExecuted(cmd);
    }


    /**
     *
     * @param cmd the command that was just executed
     */
    public void fireAfterCommandExecuted(Command cmd) {
        for (CommandEventListener cel : commandEventListeners)
            cel.afterCommandExecuted(cmd);
    }

    
    public void addRowEventListener(RowEventListener listener) {
        rowEventListeners.add(listener);
    }


    public void fireBeforeRowInserted(TableFileInfo tblFileInfo,
                                      Tuple newValues) {
        logger.debug("Firing beforeRowInserted");
        for (RowEventListener rel : rowEventListeners)
            rel.beforeRowInserted(tblFileInfo, newValues);
    }
    
    
    public void fireAfterRowInserted(TableFileInfo tblFileInfo,
                                     Tuple newTuple) {
        logger.debug("Firing afterRowInserted");
        for (RowEventListener rel : rowEventListeners)
            rel.afterRowInserted(tblFileInfo, newTuple);
    }


    public void fireBeforeRowUpdated(TableFileInfo tblFileInfo, Tuple oldTuple,
                                     Tuple newValues) {
        logger.debug("Firing beforeRowUpdated");
        for (RowEventListener rel : rowEventListeners)
            rel.beforeRowUpdated(tblFileInfo, oldTuple, newValues);
    }


    public void fireAfterRowUpdated(TableFileInfo tblFileInfo, Tuple oldValues,
                                    Tuple newTuple) {
        logger.debug("Firing afterRowUpdated");
        for (RowEventListener rel : rowEventListeners)
            rel.afterRowUpdated(tblFileInfo, oldValues, newTuple);
    }


    public void fireBeforeRowDeleted(TableFileInfo tblFileInfo,
                                     Tuple oldTuple) {
        logger.debug("Firing beforeRowDeleted");
        for (RowEventListener rel : rowEventListeners)
            rel.beforeRowDeleted(tblFileInfo, oldTuple);
    }


    public void fireAfterRowDeleted(TableFileInfo tblFileInfo,
                                    Tuple oldValues) {
        logger.debug("Firing afterRowDeleted");
        for (RowEventListener rel : rowEventListeners)
            rel.afterRowDeleted(tblFileInfo, oldValues);
    }
}
