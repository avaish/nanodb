package edu.caltech.nanodb.server;


import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.StringReader;

import java.net.Socket;

import edu.caltech.nanodb.client.SessionState;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.commands.ExitCommand;
import edu.caltech.nanodb.commands.SelectCommand;

import edu.caltech.nanodb.sqlparse.NanoSqlLexer;
import edu.caltech.nanodb.sqlparse.NanoSqlParser;


public class ClientHandler implements Runnable {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(ClientHandler.class);

    /** The unique ID assigned to this client. */
    private int id;

    /** The socket this client-handler uses to interact with its client. */
    private Socket sock;


    private ObjectInputStream objectInput;


    private ObjectOutputStream objectOutput;


    private ByteArrayOutputStream commandOutput;


    private PrintStream printOutput;


    /**
     * Initialize a new client handler with the specified ID and socket.
     *
     * @param id The unique ID assigned to this client.
     * @param sock The socket used to communicate with the client.
     */
    public ClientHandler(int id, Socket sock) {
        this.id = id;
        this.sock = sock;

        commandOutput = new ByteArrayOutputStream();
        printOutput = new PrintStream(commandOutput);
    }


    /**
     * This is the main loop that handles the commands from the client.
     */
    @Override
    public void run() {
        try {
            objectOutput = new ObjectOutputStream(sock.getOutputStream());
            objectInput = new ObjectInputStream(sock.getInputStream());

            SessionState.get().setOutputStream(printOutput);

            objectOutput.writeObject(
                "Welcome to NanoDB.  Exit with EXIT or QUIT command.\n");

            while (true) {
                // Receive one or more commands from the client.
                String commands;
                try {
                    commands = (String) objectInput.readObject();
                }
                catch (EOFException e) {
                    logger.info(String.format("Client %d disconnected.%n", id));
                    break;
                }
                catch (Exception e) {
                    // This could be an IOException, a ClassNotFoundException,
                    // or a ClassCastException.
                    logger.error(String.format(
                        "Error communicating with client %d!  Disconnecting.%n",
                        id), e);
                    break;
                }

                // Try to execute the command, and send the response back to the
                // client.

                doCommands(commands);
            }
        }
        catch (IOException e) {
            logger.error(String.format(
                "Couldn't establish communications with client %d!%n", id), e);
        }
    }


    private void doCommands(String commands) {
        StringReader strReader = new StringReader(commands);
        NanoSqlLexer lexer = new NanoSqlLexer(strReader);
        NanoSqlParser parser = new NanoSqlParser(lexer);

        while (true) {
            try {
                Exception executionException = null;
                try {
                    Command cmd = parser.command();
                    logger.debug("Parsed command:  " + cmd);
    
                    if (cmd == null || cmd instanceof ExitCommand)
                        break;

                    if (cmd instanceof SelectCommand) {
                        // Set up the SELECT command to send the tuples back to the
                        // client.
                        SelectCommand selCmd = (SelectCommand) cmd;
                        selCmd.setTupleProcessor(new TupleSender(objectOutput));
                    }
                
                    cmd.execute();
                }
                catch (Exception e) {
                    executionException = e;
                }
                
                if (executionException == null) {
                    logger.error("Encountered error during command execution",
                        executionException);
                    objectOutput.writeObject(commandOutput.toString("US-ASCII"));
                    commandOutput.reset();
                }
                else {
                    objectOutput.writeObject(executionException);
                }

                objectOutput.flush();
            }
            catch (IOException e) {
                logger.error("Couldn't send data to client.", e);
                break;
            }
        }
        
        /***
        // Persist all database changes.
        try {
            StorageManager.getInstance().closeAllOpenTables();
        }
        catch (IOException e) {
            System.out.println("IO error while closing open tables:  " +
                e.getMessage());
            logger.error("IO error while closing open tables", e);
        }
         ***/
    }
}
