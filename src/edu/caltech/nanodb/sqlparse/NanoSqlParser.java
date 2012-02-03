// $ANTLR 2.7.7 (20060906): "nanosql.g" -> "NanoSqlParser.java"$

  /**
   * Copyright (c) 2005-2011 by the California Institute of Technology.
   * All rights reserved.
   */
  package edu.caltech.nanodb.sqlparse;

  import java.util.ArrayList;
  import java.util.List;

  import edu.caltech.nanodb.commands.*;
  import edu.caltech.nanodb.expressions.*;
  import edu.caltech.nanodb.relations.*;

import antlr.TokenBuffer;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.ANTLRException;
import antlr.LLkParser;
import antlr.Token;
import antlr.TokenStream;
import antlr.RecognitionException;
import antlr.NoViableAltException;
import antlr.MismatchedTokenException;
import antlr.SemanticException;
import antlr.ParserSharedInputState;
import antlr.collections.impl.BitSet;

/**
 * A parser for processing SQL commands into the various command-classes derived
 * from {@link edu.caltech.nanodb.commands.Command}.  The information in these
 * commands is then used for data-definition, data-manipulation, and general
 * utility operations, within the database.
 */
public class NanoSqlParser extends antlr.LLkParser       implements NanoSqlParserTokenTypes
 {

protected NanoSqlParser(TokenBuffer tokenBuf, int k) {
  super(tokenBuf,k);
  tokenNames = _tokenNames;
}

public NanoSqlParser(TokenBuffer tokenBuf) {
  this(tokenBuf,2);
}

protected NanoSqlParser(TokenStream lexer, int k) {
  super(lexer,k);
  tokenNames = _tokenNames;
}

public NanoSqlParser(TokenStream lexer) {
  this(lexer,2);
}

public NanoSqlParser(ParserSharedInputState state) {
  super(state,2);
  tokenNames = _tokenNames;
}

	public final List<Command>  commands() throws RecognitionException, TokenStreamException {
		List<Command> cmds;
		
		
		cmds = new ArrayList<Command>();
		Command c1, ci;
		
		
		try {      // for error handling
			c1=command();
			cmds.add(c1);
			{
			_loop4:
			do {
				if ((LA(1)==SEMICOLON)) {
					match(SEMICOLON);
					{
					switch ( LA(1)) {
					case ANALYZE:
					case BEGIN:
					case COMMIT:
					case CREATE:
					case DELETE:
					case DROP:
					case EXIT:
					case EXPLAIN:
					case INSERT:
					case QUIT:
					case ROLLBACK:
					case SELECT:
					case START:
					case UPDATE:
					{
						ci=command();
						cmds.add(ci);
						break;
					}
					case EOF:
					case SEMICOLON:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
				}
				else {
					break _loop4;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_0);
		}
		return cmds;
	}
	
/**
 * A single statement, which could be one of many possible options.  Note that
 * this command is not followed by a semicolon, which allows it to be used in the
 * "commands" rule.
 */
	public final Command  command() throws RecognitionException, TokenStreamException {
		Command c;
		
		c = null;
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case CREATE:
			{
				c=create_stmt();
				break;
			}
			case DROP:
			{
				c=drop_stmt();
				break;
			}
			case SELECT:
			{
				c=select_stmt();
				break;
			}
			case INSERT:
			{
				c=insert_stmt();
				break;
			}
			case UPDATE:
			{
				c=update_stmt();
				break;
			}
			case DELETE:
			{
				c=delete_stmt();
				break;
			}
			case BEGIN:
			case START:
			{
				c=begin_txn_stmt();
				break;
			}
			case COMMIT:
			{
				c=commit_txn_stmt();
				break;
			}
			case ROLLBACK:
			{
				c=rollback_txn_stmt();
				break;
			}
			case ANALYZE:
			{
				c=analyze_stmt();
				break;
			}
			case EXPLAIN:
			{
				c=explain_stmt();
				break;
			}
			case EXIT:
			case QUIT:
			{
				c=exit_stmt();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final Command  command_semicolon() throws RecognitionException, TokenStreamException {
		Command c;
		
		c = null;
		
		try {      // for error handling
			c=command();
			match(SEMICOLON);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_0);
		}
		return c;
	}
	
/**
 * CREATE Statements - each database object that can be created will produce a
 * different {@link edu.caltech.nanodb.commands.Command} instance that contains
 * the SQL command's details.  This rule returns that Command instance, fully
 * configured.
 */
	public final Command  create_stmt() throws RecognitionException, TokenStreamException {
		Command c;
		
		c = null;
		
		try {      // for error handling
			if ((LA(1)==CREATE) && (LA(2)==TABLE||LA(2)==TEMPORARY)) {
				c=create_table();
			}
			else if ((LA(1)==CREATE) && (LA(2)==VIEW)) {
				c=create_view();
			}
			else if ((LA(1)==CREATE) && (LA(2)==INDEX||LA(2)==UNIQUE)) {
				c=create_index();
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final Command  drop_stmt() throws RecognitionException, TokenStreamException {
		Command c;
		
		c = null;
		
		try {      // for error handling
			c=drop_table_stmt();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final QueryCommand  select_stmt() throws RecognitionException, TokenStreamException {
		QueryCommand c;
		
		c = null; SelectClause sc = null;
		
		try {      // for error handling
			sc=select_clause();
			c = new SelectCommand(sc);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final QueryCommand  insert_stmt() throws RecognitionException, TokenStreamException {
		QueryCommand c;
		
		
		c = null;
		String name = null;
		ArrayList<String> cols = null;
		ArrayList<Expression> exprs = null;
		SelectClause sc = null;
		
		
		try {      // for error handling
			match(INSERT);
			match(INTO);
			name=dbobj_ident();
			cols=insert_cols();
			{
			switch ( LA(1)) {
			case VALUES:
			{
				exprs=insert_vals();
				c = new InsertCommand(name, cols, exprs);
				break;
			}
			case SELECT:
			{
				sc=select_clause();
				c = new InsertCommand(name, cols, sc);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final QueryCommand  update_stmt() throws RecognitionException, TokenStreamException {
		QueryCommand c;
		
		
		c = null;
		UpdateCommand uc = null;
		String name = null;
		Expression e = null;
		
		
		try {      // for error handling
			match(UPDATE);
			name=dbobj_ident();
			uc = new UpdateCommand(name); c = uc;
			match(SET);
			name=dbobj_ident();
			match(EQUALS);
			e=expression();
			uc.addValue(name, e);
			{
			_loop103:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					name=dbobj_ident();
					match(EQUALS);
					e=expression();
					uc.addValue(name, e);
				}
				else {
					break _loop103;
				}
				
			} while (true);
			}
			{
			switch ( LA(1)) {
			case WHERE:
			{
				match(WHERE);
				e=expression();
				uc.setWhereExpr(e);
				break;
			}
			case EOF:
			case SEMICOLON:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final QueryCommand  delete_stmt() throws RecognitionException, TokenStreamException {
		QueryCommand c;
		
		
		c = null;
		String name = null;
		Expression e = null;
		
		
		try {      // for error handling
			match(DELETE);
			match(FROM);
			name=dbobj_ident();
			{
			switch ( LA(1)) {
			case WHERE:
			{
				match(WHERE);
				e=expression();
				break;
			}
			case EOF:
			case SEMICOLON:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new DeleteCommand(name, e);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final BeginTransactionCommand  begin_txn_stmt() throws RecognitionException, TokenStreamException {
		BeginTransactionCommand c;
		
		c = null;
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case START:
			{
				match(START);
				match(TRANSACTION);
				break;
			}
			case BEGIN:
			{
				match(BEGIN);
				{
				switch ( LA(1)) {
				case WORK:
				{
					match(WORK);
					break;
				}
				case EOF:
				case SEMICOLON:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new BeginTransactionCommand();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final CommitTransactionCommand  commit_txn_stmt() throws RecognitionException, TokenStreamException {
		CommitTransactionCommand c;
		
		c = null;
		
		try {      // for error handling
			match(COMMIT);
			{
			switch ( LA(1)) {
			case WORK:
			{
				match(WORK);
				break;
			}
			case EOF:
			case SEMICOLON:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new CommitTransactionCommand();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final RollbackTransactionCommand  rollback_txn_stmt() throws RecognitionException, TokenStreamException {
		RollbackTransactionCommand c;
		
		c = null;
		
		try {      // for error handling
			match(ROLLBACK);
			{
			switch ( LA(1)) {
			case WORK:
			{
				match(WORK);
				break;
			}
			case EOF:
			case SEMICOLON:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new RollbackTransactionCommand();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final AnalyzeCommand  analyze_stmt() throws RecognitionException, TokenStreamException {
		AnalyzeCommand c;
		
		
		c = null;
		boolean verbose = false;
		String tblName = null;
		
		
		try {      // for error handling
			match(ANALYZE);
			{
			switch ( LA(1)) {
			case VERBOSE:
			{
				match(VERBOSE);
				verbose = true;
				break;
			}
			case IDENT:
			case QUOTED_IDENT:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			tblName=dbobj_ident();
			c = new AnalyzeCommand(tblName, verbose);
			{
			_loop117:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					tblName=dbobj_ident();
					c.addTable(tblName);
				}
				else {
					break _loop117;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final Command  explain_stmt() throws RecognitionException, TokenStreamException {
		Command c;
		
		
		c = null;
		QueryCommand cmdToExplain = null;
		
		
		try {      // for error handling
			match(EXPLAIN);
			{
			switch ( LA(1)) {
			case SELECT:
			{
				cmdToExplain=select_stmt();
				break;
			}
			case INSERT:
			{
				cmdToExplain=insert_stmt();
				break;
			}
			case UPDATE:
			{
				cmdToExplain=update_stmt();
				break;
			}
			case DELETE:
			{
				cmdToExplain=delete_stmt();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new ExplainCommand(cmdToExplain);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
/**
 * The exit command is just a simple way for the main loop to know that it's
 * time to stop having fun.
 */
	public final Command  exit_stmt() throws RecognitionException, TokenStreamException {
		Command c;
		
		c = null;
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case EXIT:
			{
				match(EXIT);
				break;
			}
			case QUIT:
			{
				match(QUIT);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			c = new ExitCommand();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
/**
 * An UNQUALIFIED identifier for a "database object" - a table or column.
 */
	public final String  dbobj_ident() throws RecognitionException, TokenStreamException {
		String s;
		
		Token  i = null;
		Token  qi = null;
		s = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case IDENT:
			{
				i = LT(1);
				match(IDENT);
				s = i.getText();
				break;
			}
			case QUOTED_IDENT:
			{
				qi = LT(1);
				match(QUOTED_IDENT);
				s = qi.getText();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_2);
		}
		return s;
	}
	
/**
 * Column names may be of the form <tt>colName</tt>, or
 * <tt>tblName.colName</tt>.
 */
	public final ColumnName  column_name() throws RecognitionException, TokenStreamException {
		ColumnName cn;
		
		
		cn = new ColumnName();
		String n1 = null;
		String n2 = null;
		
		
		try {      // for error handling
			n1=dbobj_ident();
			cn.setColumnName(n1);
			{
			switch ( LA(1)) {
			case PERIOD:
			{
				match(PERIOD);
				n2=dbobj_ident();
				cn.setTableName(n1); cn.setColumnName(n2);
				break;
			}
			case EOF:
			case AND:
			case AS:
			case ASC:
			case BETWEEN:
			case CROSS:
			case DESC:
			case FROM:
			case FULL:
			case IN:
			case INNER:
			case JOIN:
			case LEFT:
			case LIKE:
			case NATURAL:
			case NOT:
			case OR:
			case ORDER:
			case RIGHT:
			case SIMILAR:
			case WHERE:
			case SEMICOLON:
			case IDENT:
			case QUOTED_IDENT:
			case COMMA:
			case RPAREN:
			case GROUP:
			case HAVING:
			case STAR:
			case EQUALS:
			case NOT_EQUALS:
			case GRTR_THAN:
			case LESS_THAN:
			case GRTR_EQUAL:
			case LESS_EQUAL:
			case PLUS:
			case MINUS:
			case SLASH:
			case PERCENT:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_3);
		}
		return cn;
	}
	
	public final CreateTableCommand  create_table() throws RecognitionException, TokenStreamException {
		CreateTableCommand c;
		
		
		c = null;
		String name = null;
		boolean temp = false;
		boolean ifNotExists = false;
		
		
		try {      // for error handling
			match(CREATE);
			{
			switch ( LA(1)) {
			case TEMPORARY:
			{
				match(TEMPORARY);
				temp = true;
				break;
			}
			case TABLE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(TABLE);
			{
			switch ( LA(1)) {
			case IF:
			{
				match(IF);
				match(NOT);
				match(EXISTS);
				ifNotExists = true;
				break;
			}
			case IDENT:
			case QUOTED_IDENT:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			name=dbobj_ident();
			c = new CreateTableCommand(name, temp, ifNotExists);
			table_decl(c);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final CreateViewCommand  create_view() throws RecognitionException, TokenStreamException {
		CreateViewCommand c;
		
		
		c = null;
		String name = null;
		SelectClause sc = null;
		
		
		try {      // for error handling
			match(CREATE);
			match(VIEW);
			name=dbobj_ident();
			match(AS);
			sc=select_clause();
			c = new CreateViewCommand(name, sc);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final CreateIndexCommand  create_index() throws RecognitionException, TokenStreamException {
		CreateIndexCommand c;
		
		
		c = null;
		String idxType = null;
		boolean unique = false;
		String idxName = null;
		String tblName = null;
		String colName = null;
		
		
		try {      // for error handling
			match(CREATE);
			{
			switch ( LA(1)) {
			case UNIQUE:
			{
				match(UNIQUE);
				unique = true;
				break;
			}
			case INDEX:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(INDEX);
			{
			switch ( LA(1)) {
			case IDENT:
			case QUOTED_IDENT:
			{
				idxName=dbobj_ident();
				break;
			}
			case ON:
			case USING:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case USING:
			{
				match(USING);
				idxType=dbobj_ident();
				break;
			}
			case ON:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(ON);
			tblName=dbobj_ident();
			
			c = new CreateIndexCommand(idxName, tblName, unique);
			
			if (idxType != null) {
			c.setIndexType(idxType);
			}
			
			match(LPAREN);
			colName=dbobj_ident();
			c.addColumn(colName);
			{
			_loop50:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					colName=dbobj_ident();
					c.addColumn(colName);
				}
				else {
					break _loop50;
				}
				
			} while (true);
			}
			match(RPAREN);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
/**
 * Parse a comma-delimited list of column-declarations, and add them to the
 * passed-in CreateTableCommand object.  Semantic checks are done along the way
 * to ensure that none of the values are contradictory or otherwise insane.
 */
	public final void table_decl(
		CreateTableCommand ct
	) throws RecognitionException, TokenStreamException {
		
		
		try {      // for error handling
			
			ColumnInfo colInfo = null;      // Column-declarations.
			ConstraintDecl tc = null;  // Table-level constraints.
			
			match(LPAREN);
			{
			switch ( LA(1)) {
			case IDENT:
			{
				colInfo=table_col_decl(ct);
				ct.addColumn(colInfo);
				break;
			}
			case CONSTRAINT:
			case FOREIGN:
			case PRIMARY:
			case UNIQUE:
			{
				tc=table_constraint();
				ct.addConstraint(tc);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			_loop21:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					{
					switch ( LA(1)) {
					case IDENT:
					{
						colInfo=table_col_decl(ct);
						ct.addColumn(colInfo);
						break;
					}
					case CONSTRAINT:
					case FOREIGN:
					case PRIMARY:
					case UNIQUE:
					{
						tc=table_constraint();
						ct.addConstraint(tc);
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
				}
				else {
					break _loop21;
				}
				
			} while (true);
			}
			match(RPAREN);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
	}
	
/**
 * Table column declarations are similar to view column declarations, but can
 * have additional syntax declaring constraints on values in the table-column.
 */
	public final ColumnInfo  table_col_decl(
		CreateTableCommand cTab
	) throws RecognitionException, TokenStreamException {
		ColumnInfo colInfo;
		
		Token  nm = null;
		
		colInfo = null;
		ColumnType colType = null;
		ConstraintDecl con = null;  // Column constraint(s), if any are specified.
		
		
		try {      // for error handling
			nm = LT(1);
			match(IDENT);
			colType=column_type();
			colInfo = new ColumnInfo(nm.getText(), colType);
			{
			_loop24:
			do {
				if ((_tokenSet_4.member(LA(1)))) {
					con=column_constraint();
					
					con.addColumn(nm.getText());
					cTab.addConstraint(con);
					
				}
				else {
					break _loop24;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_5);
		}
		return colInfo;
	}
	
/**
 * Table columns can have a number of constraints, which may optionally be named.
 * Note that column-constraints and table-constraints can be quite different,
 * even though they are represented with the same Java class in the
 * implementation.
 */
	public final ConstraintDecl  table_constraint() throws RecognitionException, TokenStreamException {
		ConstraintDecl c;
		
		Token  cn = null;
		Token  c1 = null;
		Token  ci = null;
		Token  fkc1 = null;
		Token  fkci = null;
		Token  rtn = null;
		Token  rtc1 = null;
		Token  rtci = null;
		
		c = null;
		String cName = null;
		
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case CONSTRAINT:
			{
				match(CONSTRAINT);
				cn = LT(1);
				match(IDENT);
				cName = cn.getText();
				break;
			}
			case FOREIGN:
			case PRIMARY:
			case UNIQUE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case PRIMARY:
			case UNIQUE:
			{
				{
				switch ( LA(1)) {
				case UNIQUE:
				{
					match(UNIQUE);
					c = new ConstraintDecl(TableConstraintType.UNIQUE,      cName, false);
					break;
				}
				case PRIMARY:
				{
					match(PRIMARY);
					match(KEY);
					c = new ConstraintDecl(TableConstraintType.PRIMARY_KEY, cName, false);
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				match(LPAREN);
				c1 = LT(1);
				match(IDENT);
				c.addColumn(c1.getText());
				{
				_loop38:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						ci = LT(1);
						match(IDENT);
						c.addColumn(ci.getText());
					}
					else {
						break _loop38;
					}
					
				} while (true);
				}
				match(RPAREN);
				break;
			}
			case FOREIGN:
			{
				match(FOREIGN);
				match(KEY);
				c = new ConstraintDecl(TableConstraintType.FOREIGN_KEY, cName, false);
				match(LPAREN);
				fkc1 = LT(1);
				match(IDENT);
				c.addColumn(fkc1.getText());
				{
				_loop40:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						fkci = LT(1);
						match(IDENT);
						c.addColumn(fkci.getText());
					}
					else {
						break _loop40;
					}
					
				} while (true);
				}
				match(RPAREN);
				match(REFERENCES);
				rtn = LT(1);
				match(IDENT);
				c.setRefTable(rtn.getText());
				{
				switch ( LA(1)) {
				case LPAREN:
				{
					match(LPAREN);
					rtc1 = LT(1);
					match(IDENT);
					c.addRefColumn(rtc1.getText());
					{
					_loop43:
					do {
						if ((LA(1)==COMMA)) {
							match(COMMA);
							rtci = LT(1);
							match(IDENT);
							c.addRefColumn(rtci.getText());
						}
						else {
							break _loop43;
						}
						
					} while (true);
					}
					match(RPAREN);
					break;
				}
				case COMMA:
				case RPAREN:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_5);
		}
		return c;
	}
	
/**
 * Column type-specifications are parsed by this rule.  Some types are simple
 * keywords.  Others have supporting arguments to parse as well, such as lengths
 * or precisions.  User-defined types are not supported.
 */
	public final ColumnType  column_type() throws RecognitionException, TokenStreamException {
		ColumnType ct;
		
		Token  len = null;
		Token  len2 = null;
		
		ct = null;
		SQLDataType dt;
		
		
		try {      // for error handling
			switch ( LA(1)) {
			case TYPE_INT:
			case TYPE_INTEGER:
			{
				{
				switch ( LA(1)) {
				case TYPE_INT:
				{
					match(TYPE_INT);
					break;
				}
				case TYPE_INTEGER:
				{
					match(TYPE_INTEGER);
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				ct = new ColumnType(SQLDataType.INTEGER);
				break;
			}
			case TYPE_FLOAT:
			{
				match(TYPE_FLOAT);
				ct = new ColumnType(SQLDataType.FLOAT);
				break;
			}
			case TYPE_DOUBLE:
			{
				match(TYPE_DOUBLE);
				ct = new ColumnType(SQLDataType.DOUBLE);
				break;
			}
			case TYPE_CHAR:
			case TYPE_VARCHAR:
			{
				{
				switch ( LA(1)) {
				case TYPE_CHAR:
				{
					match(TYPE_CHAR);
					dt = SQLDataType.CHAR;
					break;
				}
				case TYPE_VARCHAR:
				{
					match(TYPE_VARCHAR);
					dt = SQLDataType.VARCHAR;
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				ct = new ColumnType(dt);
				match(LPAREN);
				len = LT(1);
				match(INT_LITERAL);
				ct.setLength(Integer.parseInt(len.getText()));
				match(RPAREN);
				break;
			}
			case TYPE_CHARACTER:
			{
				match(TYPE_CHARACTER);
				dt = SQLDataType.CHAR;
				{
				switch ( LA(1)) {
				case TYPE_VARYING:
				{
					match(TYPE_VARYING);
					dt = SQLDataType.VARCHAR;
					break;
				}
				case LPAREN:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				ct = new ColumnType(dt);
				match(LPAREN);
				len2 = LT(1);
				match(INT_LITERAL);
				ct.setLength(Integer.parseInt(len2.getText()));
				match(RPAREN);
				break;
			}
			case TYPE_DATE:
			{
				match(TYPE_DATE);
				ct = new ColumnType(SQLDataType.DATE);
				break;
			}
			case TYPE_DATETIME:
			{
				match(TYPE_DATETIME);
				ct = new ColumnType(SQLDataType.DATETIME);
				break;
			}
			case TYPE_TIME:
			{
				match(TYPE_TIME);
				ct = new ColumnType(SQLDataType.TIME);
				break;
			}
			case TYPE_TIMESTAMP:
			{
				match(TYPE_TIMESTAMP);
				ct = new ColumnType(SQLDataType.TIMESTAMP);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_6);
		}
		return ct;
	}
	
/**
 * Table columns can have a number of constraints, which may optionally be named.
 * Note that column-constraints and table-constraints can be quite different,
 * even though they are represented with the same Java class in the
 * implementation.
 */
	public final ConstraintDecl  column_constraint() throws RecognitionException, TokenStreamException {
		ConstraintDecl c;
		
		Token  cn = null;
		Token  rtn = null;
		Token  rcn = null;
		
		c = null;
		String cName = null;         // Name is optional.
		
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case CONSTRAINT:
			{
				match(CONSTRAINT);
				cn = LT(1);
				match(IDENT);
				cName = cn.getText();
				break;
			}
			case NOT:
			case PRIMARY:
			case REFERENCES:
			case UNIQUE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case NOT:
			{
				match(NOT);
				match(NULL);
				c = new ConstraintDecl(TableConstraintType.NOT_NULL,    cName, true);
				break;
			}
			case UNIQUE:
			{
				match(UNIQUE);
				c = new ConstraintDecl(TableConstraintType.UNIQUE,      cName, true);
				break;
			}
			case PRIMARY:
			{
				match(PRIMARY);
				match(KEY);
				c = new ConstraintDecl(TableConstraintType.PRIMARY_KEY, cName, true);
				break;
			}
			case REFERENCES:
			{
				match(REFERENCES);
				c = new ConstraintDecl(TableConstraintType.FOREIGN_KEY, cName, true);
				rtn = LT(1);
				match(IDENT);
				c.setRefTable(rtn.getText());
				{
				switch ( LA(1)) {
				case LPAREN:
				{
					match(LPAREN);
					rcn = LT(1);
					match(IDENT);
					c.addRefColumn(rcn.getText());
					match(RPAREN);
					break;
				}
				case CONSTRAINT:
				case NOT:
				case PRIMARY:
				case REFERENCES:
				case UNIQUE:
				case COMMA:
				case RPAREN:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_6);
		}
		return c;
	}
	
/**
 * This rule parses a SELECT clause.  Since SELECT clauses can be nested in
 * other expressions, it's important to have this as a separate sub-rule in the
 * parser.
 */
	public final SelectClause  select_clause() throws RecognitionException, TokenStreamException {
		SelectClause sc;
		
		
		sc = new SelectClause();
		SelectValue sv = null;
		FromClause fc = null;
		Expression e = null;
		boolean ascending;
		
		
		try {      // for error handling
			match(SELECT);
			{
			switch ( LA(1)) {
			case ALL:
			{
				match(ALL);
				break;
			}
			case DISTINCT:
			{
				match(DISTINCT);
				sc.setDistinct(true);
				break;
			}
			case COUNT:
			case EXISTS:
			case FALSE:
			case NOT:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case STAR:
			case PLUS:
			case MINUS:
			case STRING_LITERAL:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			sv=select_value();
			sc.addSelectValue(sv);
			{
			_loop58:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					sv=select_value();
					sc.addSelectValue(sv);
				}
				else {
					break _loop58;
				}
				
			} while (true);
			}
			{
			switch ( LA(1)) {
			case FROM:
			{
				match(FROM);
				fc=from_clause();
				sc.setFromClause(fc);
				break;
			}
			case EOF:
			case ORDER:
			case WHERE:
			case SEMICOLON:
			case RPAREN:
			case GROUP:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case WHERE:
			{
				match(WHERE);
				e=expression();
				sc.setWhereExpr(e);
				break;
			}
			case EOF:
			case ORDER:
			case SEMICOLON:
			case RPAREN:
			case GROUP:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case GROUP:
			{
				match(GROUP);
				match(BY);
				e=expression();
				sc.addGroupByExpr(e);
				{
				_loop63:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						e=expression();
						sc.addGroupByExpr(e);
					}
					else {
						break _loop63;
					}
					
				} while (true);
				}
				{
				switch ( LA(1)) {
				case HAVING:
				{
					match(HAVING);
					e=expression();
					sc.setHavingExpr(e);
					break;
				}
				case EOF:
				case ORDER:
				case SEMICOLON:
				case RPAREN:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			case EOF:
			case ORDER:
			case SEMICOLON:
			case RPAREN:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case ORDER:
			{
				match(ORDER);
				match(BY);
				e=expression();
				ascending = true;
				{
				switch ( LA(1)) {
				case ASC:
				{
					match(ASC);
					break;
				}
				case DESC:
				{
					match(DESC);
					ascending = false;
					break;
				}
				case EOF:
				case SEMICOLON:
				case COMMA:
				case RPAREN:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				sc.addOrderByExpr(new OrderByExpression(e, ascending));
				{
				_loop69:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						e=expression();
						ascending = true;
						{
						switch ( LA(1)) {
						case ASC:
						{
							match(ASC);
							break;
						}
						case DESC:
						{
							match(DESC);
							ascending = false;
							break;
						}
						case EOF:
						case SEMICOLON:
						case COMMA:
						case RPAREN:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						sc.addOrderByExpr(new OrderByExpression(e, ascending));
					}
					else {
						break _loop69;
					}
					
				} while (true);
				}
				break;
			}
			case EOF:
			case SEMICOLON:
			case RPAREN:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_7);
		}
		return sc;
	}
	
	public final DropTableCommand  drop_table_stmt() throws RecognitionException, TokenStreamException {
		DropTableCommand c;
		
		
		c = null;
		String name = null;
		boolean ifExists = false;
		
		
		try {      // for error handling
			match(DROP);
			match(TABLE);
			{
			switch ( LA(1)) {
			case IF:
			{
				match(IF);
				match(EXISTS);
				ifExists = true;
				break;
			}
			case IDENT:
			case QUOTED_IDENT:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			name=dbobj_ident();
			c = new DropTableCommand(name, ifExists);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return c;
	}
	
	public final SelectValue  select_value() throws RecognitionException, TokenStreamException {
		SelectValue sv;
		
		
		sv = null;
		Expression e = null;
		String n = null;
		SelectClause sc = null;
		
		
		try {      // for error handling
			switch ( LA(1)) {
			case STAR:
			{
				match(STAR);
				sv = new SelectValue(new ColumnName());
				break;
			}
			case COUNT:
			case EXISTS:
			case FALSE:
			case NOT:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case PLUS:
			case MINUS:
			case STRING_LITERAL:
			{
				e=expression();
				{
				switch ( LA(1)) {
				case AS:
				case IDENT:
				case QUOTED_IDENT:
				{
					{
					switch ( LA(1)) {
					case AS:
					{
						match(AS);
						break;
					}
					case IDENT:
					case QUOTED_IDENT:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					n=dbobj_ident();
					break;
				}
				case EOF:
				case FROM:
				case ORDER:
				case WHERE:
				case SEMICOLON:
				case COMMA:
				case RPAREN:
				case GROUP:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				sv = new SelectValue(e, n);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_8);
		}
		return sv;
	}
	
	public final FromClause  from_clause() throws RecognitionException, TokenStreamException {
		FromClause fc;
		
		
		fc = null;
		FromClause next = null;
		
		
		try {      // for error handling
			fc=join_expr();
			{
			_loop75:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					next=join_expr();
					fc = new FromClause(fc, next, JoinType.CROSS);
				}
				else {
					break _loop75;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_9);
		}
		return fc;
	}
	
/**
 * The expression rule matches pretty much any possible logical and/or
 * mathematical expression that one might need.  Note that it will parse a lot
 * of expressions that don't make any sense because of type-matching
 * requirements, but that is fine - this parse rule is about determining the
 * appropriate structure of the expression, and that is about applying operator
 * precedence and following the form of the expressions.  Semantic analysis
 * catches the nonsensical statements.
 */
	public final Expression  expression() throws RecognitionException, TokenStreamException {
		Expression e;
		
		e = null;
		
		try {      // for error handling
			e=logical_or_expr();
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_10);
		}
		return e;
	}
	
	public final FromClause  join_expr() throws RecognitionException, TokenStreamException {
		FromClause fc;
		
		
		fc = null;
		
		FromClause right = null;
		JoinType type = JoinType.INNER;
		
		boolean natural = false;
		Expression e = null;
		String n = null;
		
		
		try {      // for error handling
			fc=from_expr();
			{
			_loop87:
			do {
				if ((_tokenSet_11.member(LA(1)))) {
					{
					switch ( LA(1)) {
					case CROSS:
					{
						match(CROSS);
						type = JoinType.CROSS;
						break;
					}
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case NATURAL:
					case RIGHT:
					{
						{
						switch ( LA(1)) {
						case NATURAL:
						{
							match(NATURAL);
							natural = true;
							break;
						}
						case FULL:
						case INNER:
						case JOIN:
						case LEFT:
						case RIGHT:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						{
						switch ( LA(1)) {
						case INNER:
						{
							match(INNER);
							type = JoinType.INNER;
							break;
						}
						case FULL:
						case LEFT:
						case RIGHT:
						{
							{
							switch ( LA(1)) {
							case LEFT:
							{
								match(LEFT);
								type = JoinType.LEFT_OUTER;
								break;
							}
							case RIGHT:
							{
								match(RIGHT);
								type = JoinType.RIGHT_OUTER;
								break;
							}
							case FULL:
							{
								match(FULL);
								type = JoinType.FULL_OUTER;
								break;
							}
							default:
							{
								throw new NoViableAltException(LT(1), getFilename());
							}
							}
							}
							{
							switch ( LA(1)) {
							case OUTER:
							{
								match(OUTER);
								break;
							}
							case JOIN:
							{
								break;
							}
							default:
							{
								throw new NoViableAltException(LT(1), getFilename());
							}
							}
							}
							break;
						}
						case JOIN:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					{
					match(JOIN);
					right=from_expr();
					
					fc = new FromClause(fc, right, type);
					if (natural)
					fc.setConditionType(FromClause.JoinConditionType.NATURAL_JOIN);
					
					{
					switch ( LA(1)) {
					case ON:
					{
						match(ON);
						e=expression();
						
						fc.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
						fc.setOnExpression(e);
						
						break;
					}
					case USING:
					{
						match(USING);
						match(LPAREN);
						n=dbobj_ident();
						
						fc.setConditionType(FromClause.JoinConditionType.JOIN_USING);
						fc.addUsingName(n);
						
						{
						_loop86:
						do {
							if ((LA(1)==COMMA)) {
								match(COMMA);
								n=dbobj_ident();
								fc.addUsingName(n);
							}
							else {
								break _loop86;
							}
							
						} while (true);
						}
						match(RPAREN);
						break;
					}
					case EOF:
					case CROSS:
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case NATURAL:
					case ORDER:
					case RIGHT:
					case WHERE:
					case SEMICOLON:
					case COMMA:
					case RPAREN:
					case GROUP:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					}
				}
				else {
					break _loop87;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_12);
		}
		return fc;
	}
	
	public final FromClause  from_expr() throws RecognitionException, TokenStreamException {
		FromClause fc;
		
		
		fc = null;
		SelectClause sc = null;
		String name = null;
		String alias = null;
		
		
		try {      // for error handling
			if ((LA(1)==IDENT||LA(1)==QUOTED_IDENT)) {
				name=dbobj_ident();
				{
				switch ( LA(1)) {
				case AS:
				case IDENT:
				case QUOTED_IDENT:
				{
					{
					switch ( LA(1)) {
					case AS:
					{
						match(AS);
						break;
					}
					case IDENT:
					case QUOTED_IDENT:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					alias=dbobj_ident();
					break;
				}
				case EOF:
				case CROSS:
				case FULL:
				case INNER:
				case JOIN:
				case LEFT:
				case NATURAL:
				case ON:
				case ORDER:
				case RIGHT:
				case USING:
				case WHERE:
				case SEMICOLON:
				case COMMA:
				case RPAREN:
				case GROUP:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				fc = new FromClause(name, alias);
			}
			else if ((LA(1)==LPAREN) && (LA(2)==SELECT)) {
				match(LPAREN);
				sc=select_clause();
				match(RPAREN);
				{
				switch ( LA(1)) {
				case AS:
				{
					match(AS);
					break;
				}
				case IDENT:
				case QUOTED_IDENT:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				alias=dbobj_ident();
				fc = new FromClause(sc, alias);
			}
			else if ((LA(1)==LPAREN) && (LA(2)==IDENT||LA(2)==QUOTED_IDENT||LA(2)==LPAREN)) {
				match(LPAREN);
				fc=from_clause();
				match(RPAREN);
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_13);
		}
		return fc;
	}
	
	public final ArrayList<String>  insert_cols() throws RecognitionException, TokenStreamException {
		ArrayList<String> cols;
		
		
		cols = null;
		String name = null;
		
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case LPAREN:
			{
				match(LPAREN);
				name=dbobj_ident();
				cols = new ArrayList<String>(); cols.add(name);
				{
				_loop97:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						name=dbobj_ident();
						cols.add(name);
					}
					else {
						break _loop97;
					}
					
				} while (true);
				}
				match(RPAREN);
				break;
			}
			case SELECT:
			case VALUES:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_14);
		}
		return cols;
	}
	
	public final ArrayList<Expression>  insert_vals() throws RecognitionException, TokenStreamException {
		ArrayList<Expression> exprs;
		
		
		exprs = new ArrayList<Expression>();
		Expression e = null;
		
		
		try {      // for error handling
			match(VALUES);
			match(LPAREN);
			e=expression();
			exprs.add(e);
			{
			_loop100:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					e=expression();
					exprs.add(e);
				}
				else {
					break _loop100;
				}
				
			} while (true);
			}
			match(RPAREN);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_1);
		}
		return exprs;
	}
	
	public final Expression  logical_or_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		Expression e2 = null;
		BooleanOperator boolExpr = null;
		
		
		try {      // for error handling
			e=logical_and_expr();
			{
			_loop127:
			do {
				if ((LA(1)==OR)) {
					match(OR);
					e2=logical_and_expr();
					
					if (e instanceof BooleanOperator &&
					((BooleanOperator) e).getType() == BooleanOperator.Type.OR_EXPR) {
					boolExpr = (BooleanOperator) e;
					boolExpr.addTerm(e2);
					}
					else {
					boolExpr = new BooleanOperator(BooleanOperator.Type.OR_EXPR);
					boolExpr.addTerm(e);
					boolExpr.addTerm(e2);
					e = boolExpr;
					}
					
				}
				else {
					break _loop127;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_10);
		}
		return e;
	}
	
	public final ArrayList<Expression>  param_list() throws RecognitionException, TokenStreamException {
		ArrayList<Expression> exprs;
		
		
		exprs = new ArrayList<Expression>();
		Expression e = null;
		
		
		try {      // for error handling
			match(LPAREN);
			{
			switch ( LA(1)) {
			case COUNT:
			case EXISTS:
			case FALSE:
			case NOT:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case PLUS:
			case MINUS:
			case STRING_LITERAL:
			{
				e=expression();
				exprs.add(e);
				{
				_loop124:
				do {
					if ((LA(1)==COMMA)) {
						match(COMMA);
						e=expression();
						exprs.add(e);
					}
					else {
						break _loop124;
					}
					
				} while (true);
				}
				break;
			}
			case RPAREN:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(RPAREN);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_3);
		}
		return exprs;
	}
	
	public final Expression  logical_and_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		Expression e2 = null;
		BooleanOperator boolExpr = null;
		
		
		try {      // for error handling
			e=logical_not_expr();
			{
			_loop130:
			do {
				if ((LA(1)==AND)) {
					match(AND);
					e2=logical_not_expr();
					
					if (e instanceof BooleanOperator &&
					((BooleanOperator) e).getType() == BooleanOperator.Type.AND_EXPR) {
					boolExpr = (BooleanOperator) e;
					boolExpr.addTerm(e2);
					}
					else {
					boolExpr = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
					boolExpr.addTerm(e);
					boolExpr.addTerm(e2);
					e = boolExpr;
					}
					
				}
				else {
					break _loop130;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_15);
		}
		return e;
	}
	
	public final Expression  logical_not_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		boolean notExpr = false;
		BooleanOperator boolExpr = null;
		
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case NOT:
			{
				match(NOT);
				notExpr = true;
				break;
			}
			case COUNT:
			case EXISTS:
			case FALSE:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case PLUS:
			case MINUS:
			case STRING_LITERAL:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case COUNT:
			case FALSE:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case PLUS:
			case MINUS:
			case STRING_LITERAL:
			{
				e=relational_expr();
				break;
			}
			case EXISTS:
			{
				e=exists_expr();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			
			if (notExpr) {
			boolExpr = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
			boolExpr.addTerm(e);
			e = boolExpr;
			}
			
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_16);
		}
		return e;
	}
	
/**
 * @todo Change this rule to a compare_expr, then create a relational_expr that
 *       includes compare_expr, like_expr, between_expr, in_expr, and is_expr.
 *       BUT:  this introduces nondeterminism into the parser, once you add the
 *       other alternatives.  :(  Work out a solution...
 */
	public final Expression  relational_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		Expression e2 = null, e3 = null;
		
		CompareOperator.Type cmpType = null;
		
		boolean invert = false;
		StringMatchOperator.Type matchType = null;
		
		ArrayList<Expression> values = null;
		SelectClause sc = null;
		
		
		try {      // for error handling
			e=additive_expr();
			{
			switch ( LA(1)) {
			case EQUALS:
			case NOT_EQUALS:
			case GRTR_THAN:
			case LESS_THAN:
			case GRTR_EQUAL:
			case LESS_EQUAL:
			{
				{
				{
				switch ( LA(1)) {
				case EQUALS:
				{
					match(EQUALS);
					cmpType = CompareOperator.Type.EQUALS;
					break;
				}
				case NOT_EQUALS:
				{
					match(NOT_EQUALS);
					cmpType = CompareOperator.Type.NOT_EQUALS;
					break;
				}
				case GRTR_THAN:
				{
					match(GRTR_THAN);
					cmpType = CompareOperator.Type.GREATER_THAN;
					break;
				}
				case LESS_THAN:
				{
					match(LESS_THAN);
					cmpType = CompareOperator.Type.LESS_THAN;
					break;
				}
				case GRTR_EQUAL:
				{
					match(GRTR_EQUAL);
					cmpType = CompareOperator.Type.GREATER_OR_EQUAL;
					break;
				}
				case LESS_EQUAL:
				{
					match(LESS_EQUAL);
					cmpType = CompareOperator.Type.LESS_OR_EQUAL;
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				e2=additive_expr();
				e = new CompareOperator(cmpType, e, e2);
				}
				break;
			}
			case BETWEEN:
			case IN:
			case LIKE:
			case NOT:
			case SIMILAR:
			{
				{
				switch ( LA(1)) {
				case LIKE:
				case NOT:
				case SIMILAR:
				{
					{
					switch ( LA(1)) {
					case NOT:
					{
						match(NOT);
						invert = true;
						break;
					}
					case LIKE:
					case SIMILAR:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					{
					{
					switch ( LA(1)) {
					case LIKE:
					{
						match(LIKE);
						matchType = StringMatchOperator.Type.LIKE;
						break;
					}
					case SIMILAR:
					{
						match(SIMILAR);
						match(TO);
						matchType = StringMatchOperator.Type.REGEX;
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					e2=additive_expr();
					e = new StringMatchOperator(matchType, e, e2);
					}
					break;
				}
				case BETWEEN:
				{
					{
					match(BETWEEN);
					e2=additive_expr();
					match(AND);
					e3=additive_expr();
					
					BooleanOperator b = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
					b.addTerm(new CompareOperator(CompareOperator.Type.GREATER_OR_EQUAL, e, e2));
					b.addTerm(new CompareOperator(CompareOperator.Type.LESS_OR_EQUAL, e, e3));
					e = b;
					
					}
					break;
				}
				case IN:
				{
					{
					match(IN);
					{
					if ((LA(1)==LPAREN) && (_tokenSet_17.member(LA(2)))) {
						values=param_list();
						e = new InOperator(e, values);
					}
					else if ((LA(1)==LPAREN) && (LA(2)==SELECT)) {
						match(LPAREN);
						sc=select_clause();
						match(RPAREN);
						e = new InOperator(e, sc);
					}
					else {
						throw new NoViableAltException(LT(1), getFilename());
					}
					
					}
					}
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				
				if (invert) {
				// Wrap the comparison in a NOT expression.
				BooleanOperator b = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
				b.addTerm(e);
				e = b;
				}
				
				break;
			}
			case EOF:
			case AND:
			case AS:
			case ASC:
			case CROSS:
			case DESC:
			case FROM:
			case FULL:
			case INNER:
			case JOIN:
			case LEFT:
			case NATURAL:
			case OR:
			case ORDER:
			case RIGHT:
			case WHERE:
			case SEMICOLON:
			case IDENT:
			case QUOTED_IDENT:
			case COMMA:
			case RPAREN:
			case GROUP:
			case HAVING:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_16);
		}
		return e;
	}
	
	public final Expression  exists_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		e = null; SelectClause sc = null;
		
		try {      // for error handling
			match(EXISTS);
			match(LPAREN);
			sc=select_clause();
			match(RPAREN);
			e = new ExistsOperator(sc);
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_16);
		}
		return e;
	}
	
/**
 * A numeric expression is at least one numeric term.  Multiple numeric terms
 * are added or subtracted with each other.
 */
	public final Expression  additive_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		Expression e2 = null;
		ArithmeticOperator.Type mathType = null;
		
		
		try {      // for error handling
			e=mult_expr();
			{
			_loop149:
			do {
				if ((LA(1)==PLUS||LA(1)==MINUS)) {
					{
					switch ( LA(1)) {
					case PLUS:
					{
						match(PLUS);
						mathType = ArithmeticOperator.Type.ADD;
						break;
					}
					case MINUS:
					{
						match(MINUS);
						mathType = ArithmeticOperator.Type.SUBTRACT;
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					e2=mult_expr();
					e = new ArithmeticOperator(mathType, e, e2);
				}
				else {
					break _loop149;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_18);
		}
		return e;
	}
	
/**
 * A numeric term is at least one numeric factor.  Multiple numeric factors
 * are multiplied or divided with each other.
 */
	public final Expression  mult_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		Expression e2 = null;
		ArithmeticOperator.Type mathType = null;
		
		
		try {      // for error handling
			e=unary_op_expr();
			{
			_loop153:
			do {
				if ((LA(1)==STAR||LA(1)==SLASH||LA(1)==PERCENT)) {
					{
					switch ( LA(1)) {
					case STAR:
					{
						match(STAR);
						mathType = ArithmeticOperator.Type.MULTIPLY;
						break;
					}
					case SLASH:
					{
						match(SLASH);
						mathType = ArithmeticOperator.Type.DIVIDE;
						break;
					}
					case PERCENT:
					{
						match(PERCENT);
						mathType = ArithmeticOperator.Type.REMAINDER;
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					e2=unary_op_expr();
					e = new ArithmeticOperator(mathType, e, e2);
				}
				else {
					break _loop153;
				}
				
			} while (true);
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_19);
		}
		return e;
	}
	
	public final Expression  unary_op_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		
		e = null;
		
		
		try {      // for error handling
			switch ( LA(1)) {
			case MINUS:
			{
				match(MINUS);
				e=unary_op_expr();
				
				// Implement unary negation -e as 0 - e, since I am lazy.
				e = new ArithmeticOperator(ArithmeticOperator.Type.SUBTRACT,
				new LiteralValue(new Integer(0)), e);
				
				break;
			}
			case PLUS:
			{
				match(PLUS);
				e=unary_op_expr();
				break;
			}
			case COUNT:
			case FALSE:
			case NULL:
			case TRUE:
			case INT_LITERAL:
			case LONG_LITERAL:
			case FLOAT_LITERAL:
			case DEC_LITERAL:
			case IDENT:
			case QUOTED_IDENT:
			case LPAREN:
			case STRING_LITERAL:
			{
				e=base_expr();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_3);
		}
		return e;
	}
	
	public final Expression  base_expr() throws RecognitionException, TokenStreamException {
		Expression e;
		
		Token  ival = null;
		Token  lval = null;
		Token  fval = null;
		Token  dval = null;
		Token  sval = null;
		
		e = null;
		ColumnName cn = null;
		SelectClause sc = null;
		
		
		try {      // for error handling
			switch ( LA(1)) {
			case NULL:
			{
				match(NULL);
				e = new LiteralValue(null);
				break;
			}
			case TRUE:
			{
				match(TRUE);
				e = new LiteralValue(Boolean.TRUE);
				break;
			}
			case FALSE:
			{
				match(FALSE);
				e = new LiteralValue(Boolean.FALSE);
				break;
			}
			case INT_LITERAL:
			{
				ival = LT(1);
				match(INT_LITERAL);
				e = new LiteralValue(new Integer(ival.getText()));
				break;
			}
			case LONG_LITERAL:
			{
				lval = LT(1);
				match(LONG_LITERAL);
				e = new LiteralValue(new Long(lval.getText()));
				break;
			}
			case FLOAT_LITERAL:
			{
				fval = LT(1);
				match(FLOAT_LITERAL);
				e = new LiteralValue(new Float(fval.getText()));
				break;
			}
			case DEC_LITERAL:
			{
				dval = LT(1);
				match(DEC_LITERAL);
				e = new LiteralValue(new Double(dval.getText()));
				break;
			}
			case STRING_LITERAL:
			{
				sval = LT(1);
				match(STRING_LITERAL);
				e = new LiteralValue(sval.getText());
				break;
			}
			case LPAREN:
			{
				match(LPAREN);
				{
				switch ( LA(1)) {
				case COUNT:
				case EXISTS:
				case FALSE:
				case NOT:
				case NULL:
				case TRUE:
				case INT_LITERAL:
				case LONG_LITERAL:
				case FLOAT_LITERAL:
				case DEC_LITERAL:
				case IDENT:
				case QUOTED_IDENT:
				case LPAREN:
				case PLUS:
				case MINUS:
				case STRING_LITERAL:
				{
					e=logical_or_expr();
					break;
				}
				case SELECT:
				{
					sc=select_clause();
					e = new ScalarSubquery(sc);
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				match(RPAREN);
				break;
			}
			default:
				if ((LA(1)==IDENT||LA(1)==QUOTED_IDENT) && (_tokenSet_20.member(LA(2)))) {
					cn=column_name();
					e = new ColumnValue(cn);
				}
				else if ((LA(1)==COUNT||LA(1)==IDENT||LA(1)==QUOTED_IDENT) && (LA(2)==LPAREN)) {
					e=function_call();
				}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_3);
		}
		return e;
	}
	
/**
 * A function call can refer to either a normal scalar function, or it can refer
 * to an aggregate function call.  It's up to the query executor to ensure that
 * the function actually exists, and that it's the proper type for its context.
 */
	public final FunctionCall  function_call() throws RecognitionException, TokenStreamException {
		FunctionCall f;
		
		
		f = null;
		String name = null;
		ArrayList<Expression> args = null;
		Expression e = null;
		
		
		try {      // for error handling
			{
			switch ( LA(1)) {
			case IDENT:
			case QUOTED_IDENT:
			{
				name=dbobj_ident();
				args=param_list();
				f = new FunctionCall(name, args);
				break;
			}
			case COUNT:
			{
				match(COUNT);
				name="COUNT"; args = new ArrayList<Expression>();
				match(LPAREN);
				{
				switch ( LA(1)) {
				case COUNT:
				case DISTINCT:
				case EXISTS:
				case FALSE:
				case NOT:
				case NULL:
				case TRUE:
				case INT_LITERAL:
				case LONG_LITERAL:
				case FLOAT_LITERAL:
				case DEC_LITERAL:
				case IDENT:
				case QUOTED_IDENT:
				case LPAREN:
				case PLUS:
				case MINUS:
				case STRING_LITERAL:
				{
					{
					switch ( LA(1)) {
					case DISTINCT:
					{
						match(DISTINCT);
						name="COUNT-DISTINCT";
						break;
					}
					case COUNT:
					case EXISTS:
					case FALSE:
					case NOT:
					case NULL:
					case TRUE:
					case INT_LITERAL:
					case LONG_LITERAL:
					case FLOAT_LITERAL:
					case DEC_LITERAL:
					case IDENT:
					case QUOTED_IDENT:
					case LPAREN:
					case PLUS:
					case MINUS:
					case STRING_LITERAL:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					e=expression();
					args.add(e);
					break;
				}
				case STAR:
				{
					match(STAR);
					args.add(new ColumnValue(new ColumnName()));
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				match(RPAREN);
				f = new FunctionCall(name, args);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			recover(ex,_tokenSet_3);
		}
		return f;
	}
	
	
	public static final String[] _tokenNames = {
		"<0>",
		"EOF",
		"<2>",
		"NULL_TREE_LOOKAHEAD",
		"\"add\"",
		"\"all\"",
		"\"alter\"",
		"\"analyze\"",
		"\"and\"",
		"\"any\"",
		"\"as\"",
		"\"asc\"",
		"\"avg\"",
		"\"begin\"",
		"\"between\"",
		"\"by\"",
		"\"column\"",
		"\"commit\"",
		"\"constraint\"",
		"\"count\"",
		"\"create\"",
		"\"cross\"",
		"\"default\"",
		"\"delete\"",
		"\"desc\"",
		"\"distinct\"",
		"\"drop\"",
		"\"exists\"",
		"\"exit\"",
		"\"explain\"",
		"\"false\"",
		"\"foreign\"",
		"\"from\"",
		"\"full\"",
		"\"if\"",
		"\"in\"",
		"\"index\"",
		"\"inner\"",
		"\"insert\"",
		"\"into\"",
		"\"is\"",
		"\"join\"",
		"\"key\"",
		"\"left\"",
		"\"like\"",
		"\"max\"",
		"\"min\"",
		"\"natural\"",
		"\"not\"",
		"\"null\"",
		"\"on\"",
		"\"or\"",
		"\"order\"",
		"\"outer\"",
		"\"primary\"",
		"\"quit\"",
		"\"references\"",
		"\"rename\"",
		"\"right\"",
		"\"rollback\"",
		"\"select\"",
		"\"set\"",
		"\"similar\"",
		"\"some\"",
		"\"start\"",
		"\"stddev\"",
		"\"sum\"",
		"\"table\"",
		"\"to\"",
		"\"transaction\"",
		"\"true\"",
		"\"unique\"",
		"\"unknown\"",
		"\"update\"",
		"\"using\"",
		"\"values\"",
		"\"variance\"",
		"\"verbose\"",
		"\"view\"",
		"\"where\"",
		"\"work\"",
		"\"bigint\"",
		"\"blob\"",
		"\"char\"",
		"\"character\"",
		"\"date\"",
		"\"datetime\"",
		"\"decimal\"",
		"\"float\"",
		"\"double\"",
		"\"int\"",
		"\"integer\"",
		"\"numeric\"",
		"\"text\"",
		"\"time\"",
		"\"timestamp\"",
		"\"varchar\"",
		"\"varying\"",
		"INT_LITERAL",
		"LONG_LITERAL",
		"FLOAT_LITERAL",
		"DEC_LITERAL",
		"PERIOD",
		"SEMICOLON",
		"IDENT",
		"QUOTED_IDENT",
		"TEMPORARY",
		"LPAREN",
		"COMMA",
		"RPAREN",
		"GROUP",
		"HAVING",
		"STAR",
		"EQUALS",
		"NOT_EQUALS",
		"GRTR_THAN",
		"LESS_THAN",
		"GRTR_EQUAL",
		"LESS_EQUAL",
		"PLUS",
		"MINUS",
		"SLASH",
		"PERCENT",
		"STRING_LITERAL",
		"COLON",
		"NEWLINE",
		"WS",
		"COMMENT",
		"COMPARE_OPERATOR",
		"NUM_LITERAL_OR_SYMBOL"
	};
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { 2L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = { 2L, 549755813888L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	private static final long[] mk_tokenSet_2() {
		long[] data = { 8367013192217414914L, 576456079379041280L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());
	private static final long[] mk_tokenSet_3() {
		long[] data = { 4907122778490031362L, 576447008408109056L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());
	private static final long[] mk_tokenSet_4() {
		long[] data = { 90353467524382720L, 128L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_4 = new BitSet(mk_tokenSet_4());
	private static final long[] mk_tokenSet_5() {
		long[] data = { 0L, 52776558133248L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_5 = new BitSet(mk_tokenSet_5());
	private static final long[] mk_tokenSet_6() {
		long[] data = { 90353467524382720L, 52776558133376L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_6 = new BitSet(mk_tokenSet_6());
	private static final long[] mk_tokenSet_7() {
		long[] data = { 2L, 35734127902720L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_7 = new BitSet(mk_tokenSet_7());
	private static final long[] mk_tokenSet_8() {
		long[] data = { 4503603922337794L, 123695058157568L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_8 = new BitSet(mk_tokenSet_8());
	private static final long[] mk_tokenSet_9() {
		long[] data = { 4503599627370498L, 106102872113152L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_9 = new BitSet(mk_tokenSet_9());
	private static final long[] mk_tokenSet_10() {
		long[] data = { 292885858726448130L, 267731081396224L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_10 = new BitSet(mk_tokenSet_10());
	private static final long[] mk_tokenSet_11() {
		long[] data = { 288382254787330048L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_11 = new BitSet(mk_tokenSet_11());
	private static final long[] mk_tokenSet_12() {
		long[] data = { 4503599627370498L, 123695058157568L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_12 = new BitSet(mk_tokenSet_12());
	private static final long[] mk_tokenSet_13() {
		long[] data = { 294011754321543170L, 123695058158592L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_13 = new BitSet(mk_tokenSet_13());
	private static final long[] mk_tokenSet_14() {
		long[] data = { 1152921504606846976L, 2048L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_14 = new BitSet(mk_tokenSet_14());
	private static final long[] mk_tokenSet_15() {
		long[] data = { 295137658540133378L, 267731081396224L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_15 = new BitSet(mk_tokenSet_15());
	private static final long[] mk_tokenSet_16() {
		long[] data = { 295137658540133634L, 267731081396224L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_16 = new BitSet(mk_tokenSet_16());
	private static final long[] mk_tokenSet_17() {
		long[] data = { 844426138615808L, 684594680058347584L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_17 = new BitSet(mk_tokenSet_17());
	private static final long[] mk_tokenSet_18() {
		long[] data = { 4907122778490031362L, 35733578146938880L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_18 = new BitSet(mk_tokenSet_18());
	private static final long[] mk_tokenSet_19() {
		long[] data = { 4907122778490031362L, 143819969203830784L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_19 = new BitSet(mk_tokenSet_19());
	private static final long[] mk_tokenSet_20() {
		long[] data = { 4907122778490031362L, 576447283286016000L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_20 = new BitSet(mk_tokenSet_20());
	
	}
