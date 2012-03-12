header {
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
}

/**
 * A parser for processing SQL commands into the various command-classes derived
 * from {@link edu.caltech.nanodb.commands.Command}.  The information in these
 * commands is then used for data-definition, data-manipulation, and general
 * utility operations, within the database.
 */
class NanoSqlParser extends Parser;
options {
  k = 2;
}

tokens {
  // Keywords:

  ADD         = "add";
  ALL         = "all";
  ALTER       = "alter";
  ANALYZE     = "analyze";
  AND         = "and";
  ANY         = "any";
  AS          = "as";
  ASC         = "asc";
  AVG         = "avg";
  BEGIN       = "begin";
  BETWEEN     = "between";
  BY          = "by";
  COLUMN      = "column";
  COMMIT      = "commit";
  CONSTRAINT  = "constraint";
  COUNT       = "count";
  CRASH       = "crash";
  CREATE      = "create";
  CROSS       = "cross";
  DEFAULT     = "default";
  DELETE      = "delete";
  DESC        = "desc";
  DISTINCT    = "distinct";
  DROP        = "drop";
  EXISTS      = "exists";
  EXIT        = "exit";
  EXPLAIN     = "explain";
  FALSE       = "false";
  FOREIGN     = "foreign";
  FROM        = "from";
  FULL        = "full";
  IF          = "if";
  IN          = "in";
  INDEX       = "index";
  INNER       = "inner";
  INSERT      = "insert";
  INTO        = "into";
  IS          = "is";
  JOIN        = "join";
  KEY         = "key";
  LEFT        = "left";
  LIKE        = "like";
  MAX         = "max";
  MIN         = "min";
  NATURAL     = "natural";
  NOT         = "not";
  NULL        = "null";
  ON          = "on";
  OPTIMIZE    = "optimize";
  OR          = "or";
  ORDER       = "order";
  OUTER       = "outer";
  PRIMARY     = "primary";
  QUIT        = "quit";
  REFERENCES  = "references";
  RENAME      = "rename";
  RIGHT       = "right";
  ROLLBACK    = "rollback";
  SELECT      = "select";
  SET         = "set";
  SIMILAR     = "similar";
  SOME        = "some";
  START       = "start";
  STDDEV      = "stddev";
  SUM         = "sum";
  TABLE       = "table";
  TO          = "to";
  TRANSACTION = "transaction";
  TRUE        = "true";
  UNIQUE      = "unique";
  UNKNOWN     = "unknown";
  UPDATE      = "update";
  USING       = "using";
  VALUES      = "values";
  VARIANCE    = "variance";
  VERBOSE     = "verbose";
  VERIFY      = "verify";
  VIEW        = "view";
  WHERE       = "where";
  WORK        = "work";

  // These tokens are for type-recognition.  A number of these types have
  // additional syntax for specifying length or precision, which is why we have
  // parser rules for them.  The type-system is not extensible by database users.

  TYPE_BIGINT    = "bigint";
  TYPE_BLOB      = "blob";
  TYPE_CHAR      = "char";      // char(length)
  TYPE_CHARACTER = "character"; // character(length)
  TYPE_DATE      = "date";
  TYPE_DATETIME  = "datetime";
  TYPE_DECIMAL   = "decimal";   // decimal, decimal(prec), decimal(prec, scale)
  TYPE_FLOAT     = "float";     // float, float(prec)
  TYPE_DOUBLE    = "double";    // double
  TYPE_INT       = "int";
  TYPE_INTEGER   = "integer";
  TYPE_NUMERIC   = "numeric";   // numeric, numeric(prec), numeric(prec, scale)
  TYPE_TEXT      = "text";
  TYPE_TIME      = "time";
  TYPE_TIMESTAMP = "timestamp";
  TYPE_VARCHAR   = "varchar";   // varchar(length)
  TYPE_VARYING   = "varying";   // character varying(length)

  // Intermediate tokens in the tree:

  /* NONE SO FAR. */

  // Lexer tokens that are programmatically determined:

  // (There is not a specific rule for each of these lexer tokens, but
  // they still have to be declared SOMEWHERE to get things to compile!)

  INT_LITERAL;
  LONG_LITERAL;
  FLOAT_LITERAL;
  DEC_LITERAL;
  PERIOD;
}

/* A list of one or more statements, separated by semicolons.  Multiple
 * semicolons without statements are fine, as is a single statement with
 * a semicolon and no subsequent statement.
 */
commands returns [List<Command> cmds]
  {
    cmds = new ArrayList<Command>();
    Command c1, ci;
  }
  : c1=command { cmds.add(c1); }
    (SEMICOLON (ci=command { cmds.add(ci); } )? )* ;

/* A single statement followed by a semicolon, used for the interactive parser. */
command_semicolon returns [Command c] { c = null; } : c=command SEMICOLON ;

/**
 * A single statement, which could be one of many possible options.  Note that
 * this command is not followed by a semicolon, which allows it to be used in the
 * "commands" rule.
 */
command returns [Command c] { c = null; } :
  ( c=create_stmt /* | alter_stmt */ | c=drop_stmt                 // DDL
  | c=select_stmt | c=insert_stmt | c=update_stmt | c=delete_stmt  // DML
  | c=begin_txn_stmt | c=commit_txn_stmt | c=rollback_txn_stmt     // Transactions
  | c=analyze_stmt | c=explain_stmt | c=exit_stmt                  // Utility
  | c=verify_stmt | c=optimize_stmt | c=crash_stmt                 // Utility
  )
  ;


/**
 * The exit command is just a simple way for the main loop to know that it's
 * time to stop having fun.
 */
exit_stmt returns [Command c] { c = null; } :
  ( EXIT | QUIT ) { c = new ExitCommand(); } ;


/* Some common subrules that are used by many of these commands. */

/**
 * An UNQUALIFIED identifier for a "database object" - a table or column.
 */
dbobj_ident returns [String s] { s = null; } :
    i:IDENT { s = i.getText(); }
  | qi:QUOTED_IDENT { s = qi.getText(); } ;


/**
 * Column names may be of the form <tt>colName</tt>, or
 * <tt>tblName.colName</tt>.
 */
column_name returns [ColumnName cn]
  {
    cn = new ColumnName();
    String n1 = null;
    String n2 = null;
  }
  :
  n1=dbobj_ident { cn.setColumnName(n1); }
  ( PERIOD n2=dbobj_ident { cn.setTableName(n1); cn.setColumnName(n2); } )?
  ;

/**
 * CREATE Statements - each database object that can be created will produce a
 * different {@link edu.caltech.nanodb.commands.Command} instance that contains
 * the SQL command's details.  This rule returns that Command instance, fully
 * configured.
 */
create_stmt returns [Command c] { c = null; } :
  c=create_table | c=create_view | c=create_index ;


//===== TABLES ============================


create_table returns [CreateTableCommand c]
  {
    c = null;
    String name = null;
    boolean temp = false;
    boolean ifNotExists = false;
  }
  :
  CREATE ( TEMPORARY { temp = true; } )? TABLE ( IF NOT EXISTS { ifNotExists = true; } )?
  name=dbobj_ident
  { c = new CreateTableCommand(name, temp, ifNotExists); }
  table_decl[c]
  ;

/**
 * Parse a comma-delimited list of column-declarations, and add them to the
 * passed-in CreateTableCommand object.  Semantic checks are done along the way
 * to ensure that none of the values are contradictory or otherwise insane.
 */
table_decl[CreateTableCommand ct] :
  {
    ColumnInfo colInfo = null;      // Column-declarations.
    ConstraintDecl tc = null;  // Table-level constraints.
  }
  LPAREN
    ( colInfo = table_col_decl[ct] { ct.addColumn(colInfo); }
    | tc=table_constraint { ct.addConstraint(tc); } )
    ( COMMA
      ( colInfo = table_col_decl[ct] { ct.addColumn(colInfo); }
      | tc=table_constraint { ct.addConstraint(tc); } )
    )*
  RPAREN
  ;


/**
 * Table column declarations are similar to view column declarations, but can
 * have additional syntax declaring constraints on values in the table-column.
 */
table_col_decl[CreateTableCommand cTab] returns [ColumnInfo colInfo]
  {
    colInfo = null;
    ColumnType colType = null;
    ConstraintDecl con = null;  // Column constraint(s), if any are specified.
  }
  :
  nm:IDENT colType = column_type { colInfo = new ColumnInfo(nm.getText(), colType); }
  ( con = column_constraint
    {
      con.addColumn(nm.getText());
      cTab.addConstraint(con);
    }
  )*
  ;


/**
 * Column type-specifications are parsed by this rule.  Some types are simple
 * keywords.  Others have supporting arguments to parse as well, such as lengths
 * or precisions.  User-defined types are not supported.
 */
column_type returns [ColumnType ct]
  {
    ct = null;
    SQLDataType dt;
  }
  :
    ( TYPE_INT | TYPE_INTEGER ) { ct = new ColumnType(SQLDataType.INTEGER); }
  | TYPE_FLOAT { ct = new ColumnType(SQLDataType.FLOAT); }
  | TYPE_DOUBLE { ct = new ColumnType(SQLDataType.DOUBLE); }
  | ( TYPE_CHAR { dt = SQLDataType.CHAR; } | TYPE_VARCHAR { dt = SQLDataType.VARCHAR; } )
    { ct = new ColumnType(dt); }
    LPAREN len:INT_LITERAL { ct.setLength(Integer.parseInt(len.getText())); }
    RPAREN
  | TYPE_CHARACTER { dt = SQLDataType.CHAR; } ( TYPE_VARYING { dt = SQLDataType.VARCHAR; } )?
    { ct = new ColumnType(dt); }
    LPAREN len2:INT_LITERAL { ct.setLength(Integer.parseInt(len2.getText())); }
    RPAREN
  | TYPE_DATE { ct = new ColumnType(SQLDataType.DATE); }
  | TYPE_DATETIME { ct = new ColumnType(SQLDataType.DATETIME); }
  | TYPE_TIME { ct = new ColumnType(SQLDataType.TIME); }
  | TYPE_TIMESTAMP { ct = new ColumnType(SQLDataType.TIMESTAMP); }
  ;


/**
 * Table columns can have a number of constraints, which may optionally be named.
 * Note that column-constraints and table-constraints can be quite different,
 * even though they are represented with the same Java class in the
 * implementation.
 */
column_constraint returns [ConstraintDecl c]
  {
    c = null;
    String cName = null;         // Name is optional.
  }
  :
  ( CONSTRAINT cn:IDENT { cName = cn.getText(); } )?
  ( NOT NULL    { c = new ConstraintDecl(TableConstraintType.NOT_NULL,    cName, true); }
  | UNIQUE      { c = new ConstraintDecl(TableConstraintType.UNIQUE,      cName, true); }
  | PRIMARY KEY { c = new ConstraintDecl(TableConstraintType.PRIMARY_KEY, cName, true); }
  | REFERENCES  { c = new ConstraintDecl(TableConstraintType.FOREIGN_KEY, cName, true); }
    rtn:IDENT { c.setRefTable(rtn.getText()); }
    ( LPAREN rcn:IDENT { c.addRefColumn(rcn.getText()); } RPAREN )?
  )
  ;


/**
 * Table columns can have a number of constraints, which may optionally be named.
 * Note that column-constraints and table-constraints can be quite different,
 * even though they are represented with the same Java class in the
 * implementation.
 */
table_constraint returns [ConstraintDecl c]
  {
    c = null;
    String cName = null;
  }
  :
  ( CONSTRAINT cn:IDENT { cName = cn.getText(); } )?
  (
    // UNIQUE and PRIMARY KEY constraints follow the same pattern.
    ( UNIQUE      { c = new ConstraintDecl(TableConstraintType.UNIQUE,      cName, false); }
    | PRIMARY KEY { c = new ConstraintDecl(TableConstraintType.PRIMARY_KEY, cName, false); }
    )
    LPAREN c1:IDENT { c.addColumn(c1.getText()); }
    (COMMA ci:IDENT { c.addColumn(ci.getText()); } )*
    RPAREN
  |
    // FOREIGN KEY constraints are similar to UNIQUE and PRIMARY KEY constraints,
    // but also include reference-table and reference-column information.
    FOREIGN KEY { c = new ConstraintDecl(TableConstraintType.FOREIGN_KEY, cName, false); }
    LPAREN   fkc1:IDENT { c.addColumn(fkc1.getText()); }
      (COMMA fkci:IDENT { c.addColumn(fkci.getText()); } )*
    RPAREN
    REFERENCES rtn:IDENT { c.setRefTable(rtn.getText()); }
    ( LPAREN rtc1:IDENT { c.addRefColumn(rtc1.getText()); }
      (COMMA rtci:IDENT { c.addRefColumn(rtci.getText()); } )*
      RPAREN )?
  ) ;


create_view returns [CreateViewCommand c]
  {
    c = null;
    String name = null;
    SelectClause sc = null;
  }
  :
  CREATE VIEW name=dbobj_ident AS sc=select_clause
  { c = new CreateViewCommand(name, sc); }
  ;


create_index returns [CreateIndexCommand c]
  {
    c = null;
    String idxType = null;
    boolean unique = false;
    String idxName = null;
    String tblName = null;
    String colName = null;
  }
  :
  CREATE ( UNIQUE { unique = true; } )? INDEX ( idxName=dbobj_ident )?
  ( USING idxType=dbobj_ident )?
  ON tblName=dbobj_ident
  {
    c = new CreateIndexCommand(idxName, tblName, unique);

    if (idxType != null) {
      c.setIndexType(idxType);
    }
  }
  LPAREN colName=dbobj_ident { c.addColumn(colName); }
         ( COMMA colName=dbobj_ident { c.addColumn(colName); } )* RPAREN
  ;


/* ALTER Statements */

/*
alter_stmt :
  ALTER TABLE dbobj_ident
    ( RENAME TO dbobj_ident                         // Rename table.
    | RENAME (COLUMN)? dbobj_ident TO dbobj_ident   // Rename column in table.
    )
  ;
*/

/* DROP Statements */

drop_stmt returns [Command c] { c = null; } :
  c=drop_table_stmt /* | c=drop_index_stmt | c=drop_view_stmt */ ;

drop_table_stmt returns [DropTableCommand c]
  {
    c = null;
    String name = null;
    boolean ifExists = false;
  }
  :
  DROP TABLE ( IF EXISTS { ifExists = true; } )? name=dbobj_ident
  { c = new DropTableCommand(name, ifExists); }
  ;



/* SELECT Statements */

select_stmt returns [QueryCommand c] { c = null; SelectClause sc = null; } :
  sc=select_clause { c = new SelectCommand(sc); }
  ;

/**
 * This rule parses a SELECT clause.  Since SELECT clauses can be nested in
 * other expressions, it's important to have this as a separate sub-rule in the
 * parser.
 */
select_clause returns [SelectClause sc]
  {
    sc = new SelectClause();
    SelectValue sv = null;
    FromClause fc = null;
    Expression e = null;
    boolean ascending;
  }
  :
  SELECT ( ALL | DISTINCT { sc.setDistinct(true); } )?
  sv=select_value { sc.addSelectValue(sv); }
      ( COMMA sv=select_value { sc.addSelectValue(sv); } )*

  ( FROM fc=from_clause { sc.setFromClause(fc); } )?
  ( WHERE e=expression { sc.setWhereExpr(e); } )?

  ( GROUP BY e=expression { sc.addGroupByExpr(e); }
             ( COMMA e=expression { sc.addGroupByExpr(e); } )*
    ( HAVING e=expression { sc.setHavingExpr(e); } )?
  )?

  ( ORDER BY e=expression { ascending = true; } ( ASC | DESC { ascending = false; } )?
  { sc.addOrderByExpr(new OrderByExpression(e, ascending)); }
             ( COMMA e=expression { ascending = true; } ( ASC | DESC { ascending = false; } )?
             { sc.addOrderByExpr(new OrderByExpression(e, ascending)); } )* )?
/*
  ( ( UNION | INTERSECT | EXCEPT ) (ALL)? select_stmt )?
*/
  ;

select_value returns [SelectValue sv]
  {
    sv = null;
    Expression e = null;
    String n = null;
    SelectClause sc = null;
  }
  :
    STAR { sv = new SelectValue(new ColumnName()); }
//  TODO:  Nondeterminism warning.
//  | n=dbobj_ident PERIOD STAR { sv = new SelectValue(new ColumnName(n, null)); }
  | e=expression ( (AS)? n=dbobj_ident )? { sv = new SelectValue(e, n); }
// TODO:  This should no longer be necessary, since we have scalar subqueries
//        now as expressions.  Also, we need to support scalar subqueries in the
//        WHERE clause anyway.
//  | LPAREN sc=select_clause RPAREN ( (AS)? n=dbobj_ident )? { sv = new SelectValue(sc, n); }
  ;


from_clause returns [FromClause fc]
  {
    fc = null;
    FromClause next = null;
  }
  :
  fc=join_expr
  ( COMMA next=join_expr { fc = new FromClause(fc, next, JoinType.CROSS); } )*
  ;


join_expr returns [FromClause fc]
  {
    fc = null;

    FromClause right = null;
    JoinType type = JoinType.INNER;

    boolean natural = false;
    Expression e = null;
    String n = null;
  }
  :
  fc=from_expr
  (
    ( CROSS { type = JoinType.CROSS; }
    | ( NATURAL { natural = true; } )?
      ( INNER   { type = JoinType.INNER;       }
      | ( LEFT  { type = JoinType.LEFT_OUTER;  }
        | RIGHT { type = JoinType.RIGHT_OUTER; }
        | FULL  { type = JoinType.FULL_OUTER;  }
        )
        (OUTER)?
      )?
    )
    ( JOIN right=from_expr {
        fc = new FromClause(fc, right, type);
        if (natural)
          fc.setConditionType(FromClause.JoinConditionType.NATURAL_JOIN);
      }
      ( ON e=expression {
          fc.setConditionType(FromClause.JoinConditionType.JOIN_ON_EXPR);
          fc.setOnExpression(e);
        }

      | USING LPAREN n=dbobj_ident {
          fc.setConditionType(FromClause.JoinConditionType.JOIN_USING);
          fc.addUsingName(n);
        }
        ( COMMA n=dbobj_ident { fc.addUsingName(n); } )* RPAREN
      )?
    )
  )*
  ;


from_expr returns [FromClause fc]
  {
    fc = null;
    SelectClause sc = null;
    String name = null;
    String alias = null;
  }
  :
    name=dbobj_ident ( (AS)? alias=dbobj_ident )? { fc = new FromClause(name, alias); }
  | LPAREN sc=select_clause RPAREN (AS)? alias=dbobj_ident { fc = new FromClause(sc, alias); }
  | LPAREN fc=from_clause RPAREN
  ;



/* INSERT Statements */

insert_stmt returns [QueryCommand c]
  {
    c = null;
    String name = null;
    ArrayList<String> cols = null;
    ArrayList<Expression> exprs = null;
    SelectClause sc = null;
  } :
  INSERT INTO name=dbobj_ident cols=insert_cols
  ( exprs=insert_vals { c = new InsertCommand(name, cols, exprs); }
  | sc=select_clause  { c = new InsertCommand(name, cols, sc);    } ) ;

// Optional list of columns that can appear within an INSERT command.  If the
// list isn't specified, this parse rule evaluates to null.
insert_cols returns [ArrayList<String> cols]
  {
    cols = null;
    String name = null;
  } :
  ( LPAREN name=dbobj_ident { cols = new ArrayList<String>(); cols.add(name); }
           ( COMMA name=dbobj_ident { cols.add(name); } )* RPAREN )?
  ;

insert_vals returns [ArrayList<Expression> exprs]
  {
    exprs = new ArrayList<Expression>();
    Expression e = null;
  } :
  VALUES LPAREN e=expression { exprs.add(e); }
  ( COMMA e=expression { exprs.add(e); } )* RPAREN ;


/* UPDATE Statements */

update_stmt returns [QueryCommand c]
  {
    c = null;
    UpdateCommand uc = null;
    String name = null;
    Expression e = null;
  } :
  UPDATE name=dbobj_ident { uc = new UpdateCommand(name); c = uc; }
    SET name=dbobj_ident EQUALS e=expression { uc.addValue(name, e); }
    ( COMMA name=dbobj_ident EQUALS e=expression { uc.addValue(name, e); } )*
    ( WHERE e=expression { uc.setWhereExpr(e); } )?
    ;


/* DELETE Statements */

delete_stmt returns [QueryCommand c]
  {
    c = null;
    String name = null;
    Expression e = null;
  } :
  DELETE FROM name=dbobj_ident ( WHERE e=expression )?
  { c = new DeleteCommand(name, e); }
  ;


/* Transaction-processing statements */

begin_txn_stmt returns [BeginTransactionCommand c] { c = null; } :
  (
    START TRANSACTION
  | BEGIN ( WORK )?
  )
  { c = new BeginTransactionCommand(); }
  ;

commit_txn_stmt returns [CommitTransactionCommand c] { c = null; } :
  COMMIT ( WORK )?
  { c = new CommitTransactionCommand(); }
  ;

rollback_txn_stmt returns [RollbackTransactionCommand c] { c = null; } :
  ROLLBACK ( WORK )?
  { c = new RollbackTransactionCommand(); }
  ;


/* ANALYZE Statements */

analyze_stmt returns [AnalyzeCommand c]
  {
    c = null;
    boolean verbose = false;
    String tblName = null;
  } :
  ANALYZE (VERBOSE { verbose = true; } )?
  tblName=dbobj_ident { c = new AnalyzeCommand(tblName, verbose); }
  ( COMMA tblName=dbobj_ident { c.addTable(tblName); } )*
  ;


/* EXPLAIN Statements */

explain_stmt returns [Command c]
  {
    c = null;
    QueryCommand cmdToExplain = null;
  } :
  EXPLAIN ( cmdToExplain=select_stmt | cmdToExplain=insert_stmt
          | cmdToExplain=update_stmt | cmdToExplain=delete_stmt )
  { c = new ExplainCommand(cmdToExplain); }
  ;


/* VERIFY Statements */

verify_stmt returns [VerifyCommand c]
  {
    c = null;
    String tblName = null;
  } :
  VERIFY tblName=dbobj_ident { c = new VerifyCommand(tblName); }
  ( COMMA tblName=dbobj_ident { c.addTable(tblName); } )*
  ;


/* OPTIMIZE Statements */

optimize_stmt returns [OptimizeCommand c]
  {
    c = null;
    String tblName = null;
  } :
  OPTIMIZE tblName=dbobj_ident { c = new OptimizeCommand(tblName); }
  ( COMMA tblName=dbobj_ident { c.addTable(tblName); } )*
  ;


/* CRASH Statements */

crash_stmt returns [CrashCommand c]
  { c = null; } :
  CRASH { c = new CrashCommand(); }
  ;


/**
 * The expression rule matches pretty much any possible logical and/or
 * mathematical expression that one might need.  Note that it will parse a lot
 * of expressions that don't make any sense because of type-matching
 * requirements, but that is fine - this parse rule is about determining the
 * appropriate structure of the expression, and that is about applying operator
 * precedence and following the form of the expressions.  Semantic analysis
 * catches the nonsensical statements.
 */
expression returns [Expression e] { e = null; } : e=logical_or_expr ;


param_list returns [ArrayList<Expression> exprs]
  {
    exprs = new ArrayList<Expression>();
    Expression e = null;
  }
  :
  LPAREN ( e=expression { exprs.add(e); }
  ( COMMA e=expression { exprs.add(e); } )* )? RPAREN
  ;


logical_or_expr returns [Expression e]
  {
    e = null;
    Expression e2 = null;
    BooleanOperator boolExpr = null;
  } :
  e=logical_and_expr
  ( OR e2=logical_and_expr
    {
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
  )* ;

logical_and_expr returns [Expression e]
  {
    e = null;
    Expression e2 = null;
    BooleanOperator boolExpr = null;
  } :
  e=logical_not_expr
  ( AND e2=logical_not_expr
    {
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
  )* ;

logical_not_expr returns [Expression e]
  {
    e = null;
    boolean notExpr = false;
    BooleanOperator boolExpr = null;
  } :
  (NOT { notExpr = true; } )?
  ( e=relational_expr | e=exists_expr )
  {
    if (notExpr) {
      boolExpr = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
      boolExpr.addTerm(e);
      e = boolExpr;
    }
  }
  ;


exists_expr returns [Expression e]
  { e = null; SelectClause sc = null; } :
  EXISTS LPAREN sc=select_clause RPAREN
  { e = new ExistsOperator(sc); }
  ;


/**
 * @todo Change this rule to a compare_expr, then create a relational_expr that
 *       includes compare_expr, like_expr, between_expr, in_expr, and is_expr.
 *       BUT:  this introduces nondeterminism into the parser, once you add the
 *       other alternatives.  :(  Work out a solution...
 */
relational_expr returns [Expression e]
  {
    e = null;
    Expression e2 = null, e3 = null;

    CompareOperator.Type cmpType = null;

    boolean invert = false;
    StringMatchOperator.Type matchType = null;

    ArrayList<Expression> values = null;
    SelectClause sc = null;
  } :
  e=additive_expr
  (
    (
      ( EQUALS     { cmpType = CompareOperator.Type.EQUALS;           }
      | NOT_EQUALS { cmpType = CompareOperator.Type.NOT_EQUALS;       }
      | GRTR_THAN  { cmpType = CompareOperator.Type.GREATER_THAN;     }
      | LESS_THAN  { cmpType = CompareOperator.Type.LESS_THAN;        }
      | GRTR_EQUAL { cmpType = CompareOperator.Type.GREATER_OR_EQUAL; }
      | LESS_EQUAL { cmpType = CompareOperator.Type.LESS_OR_EQUAL;    } )
      e2=additive_expr { e = new CompareOperator(cmpType, e, e2); }
    )
  | (
      ( NOT { invert = true; } )?
      (
        ( LIKE       { matchType = StringMatchOperator.Type.LIKE;  }
        | SIMILAR TO { matchType = StringMatchOperator.Type.REGEX; } )
        e2=additive_expr { e = new StringMatchOperator(matchType, e, e2); } )
      | ( BETWEEN e2=additive_expr AND e3=additive_expr
          {
            BooleanOperator b = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
            b.addTerm(new CompareOperator(CompareOperator.Type.GREATER_OR_EQUAL, e, e2));
            b.addTerm(new CompareOperator(CompareOperator.Type.LESS_OR_EQUAL, e, e3));
            e = b;
          }
        )
      | ( IN
          ( values=param_list { e = new InOperator(e, values); }
          | LPAREN sc=select_clause RPAREN { e = new InOperator(e, sc); } ) )
    )
    {
      if (invert) {
        // Wrap the comparison in a NOT expression.
        BooleanOperator b = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
        b.addTerm(e);
        e = b;
      }
    }
  )?
  ;

/*
in_expr : is_expr (NOT)? IN ( param_list | LPAREN select_stmt RPAREN ) ;

is_expr : additive_expr ( IS ( TRUE | FALSE | UNKNOWN | (NOT)? NULL ) )? ;
*/

/**
 * A numeric expression is at least one numeric term.  Multiple numeric terms
 * are added or subtracted with each other.
 */
additive_expr returns [Expression e]
  {
    e = null;
    Expression e2 = null;
    ArithmeticOperator.Type mathType = null;
  }
  :
  e=mult_expr
  ( ( PLUS  { mathType = ArithmeticOperator.Type.ADD; }
    | MINUS { mathType = ArithmeticOperator.Type.SUBTRACT; } )
    e2=mult_expr { e = new ArithmeticOperator(mathType, e, e2); } )*
  ;

/**
 * A numeric term is at least one numeric factor.  Multiple numeric factors
 * are multiplied or divided with each other.
 */
mult_expr returns [Expression e]
  {
    e = null;
    Expression e2 = null;
    ArithmeticOperator.Type mathType = null;
  }
  :
  e=unary_op_expr
  ( ( STAR    { mathType = ArithmeticOperator.Type.MULTIPLY;  }
    | SLASH   { mathType = ArithmeticOperator.Type.DIVIDE;    }
    | PERCENT { mathType = ArithmeticOperator.Type.REMAINDER; } )
    e2=unary_op_expr { e = new ArithmeticOperator(mathType, e, e2); } )*
  ;

unary_op_expr returns [Expression e]
  {
    e = null;
  }
  :
    MINUS e=unary_op_expr {
      // Implement unary negation -e as 0 - e, since I am lazy.
      e = new ArithmeticOperator(ArithmeticOperator.Type.SUBTRACT,
                                 new LiteralValue(new Integer(0)), e);
    }
  | PLUS e=unary_op_expr
  | e=base_expr
  ;

base_expr returns [Expression e]
  {
    e = null;
    ColumnName cn = null;
    SelectClause sc = null;
  }
  :
    NULL  { e = new LiteralValue(null);          }
  | TRUE  { e = new LiteralValue(Boolean.TRUE);  }
  | FALSE { e = new LiteralValue(Boolean.FALSE); }
  | ival:INT_LITERAL    { e = new LiteralValue(new Integer(ival.getText())); }
  | lval:LONG_LITERAL   { e = new LiteralValue(new Long(lval.getText()));    }
  | fval:FLOAT_LITERAL  { e = new LiteralValue(new Float(fval.getText()));   }
  | dval:DEC_LITERAL    { e = new LiteralValue(new Double(dval.getText()));  }
  | sval:STRING_LITERAL { e = new LiteralValue(sval.getText()); }
  | cn=column_name      { e = new ColumnValue(cn); }
  | e=function_call
  | LPAREN
    ( e=logical_or_expr | sc=select_clause { e = new ScalarSubquery(sc); } )
    RPAREN
  ;


/**
 * A function call can refer to either a normal scalar function, or it can refer
 * to an aggregate function call.  It's up to the query executor to ensure that
 * the function actually exists, and that it's the proper type for its context.
 */
function_call returns [FunctionCall f]
  {
    f = null;
    String name = null;
    ArrayList<Expression> args = null;
    Expression e = null;
  }
  :
  ( name=dbobj_ident args=param_list { f = new FunctionCall(name, args); }
  | COUNT { name="COUNT"; args = new ArrayList<Expression>(); }
    LPAREN
    ( (DISTINCT { name="COUNT-DISTINCT"; } )? e=expression { args.add(e); }
    | STAR { args.add(new ColumnValue(new ColumnName())); }
    )
    RPAREN
    { f = new FunctionCall(name, args); }
  )
  ;


// I have to wrap the comment for the lexer in curly-braces so that we can
// include the SuppressWarnings annotation as well.  Otherwise, ANTLR chokes.
{
/**
 * A lexer for tokenizing SQL commands into various tokens necessary for
 * parsing.  As is true for most lexers generated by ANTLR, this one primarily
 * concentrates on identifying various symbols like punctuation characters,
 * whitespace, and newlines, although it also identifies some more "high-level"
 * tokens such as identifiers and "typed" literals (i.e. literals that follow a
 * well-defined format based on their type).
 * <p>
 * All of the SQL-specific keywords are actually declared in the parser, so that
 * keeps the lexer definition pretty short and sweet.
 */
@SuppressWarnings({"unchecked", "cast"})
}
class NanoSqlLexer extends Lexer;
options {
  k = 2;
  testLiterals = false;
  caseSensitiveLiterals = false;
  defaultErrorHandler = false;
}


/* Characters */

// The PERIOD ('.'), PLUS ('+'), and MINUS ('-') symbols are detected and
// generated by the NUM_LITERAL_OR_SYMBOL lexer-rule.

// The EQUALS ('=' or '=='), NOT_EQUALS('!=' or '<>'), GRTR_THAN ('>'),
// LESS_THAN ('<'), GRTR_EQUAL ('>='), and LESS_EQUAL ('<=') symbols are all
// detected and generated by the COMPARE_OPERATOR lexer-rule.

COLON     : ':' ;
COMMA     : ',' ;
LPAREN    : '(' ;
RPAREN    : ')' ;
SEMICOLON : ';' ;
STAR      : '*' ;
SLASH     : '/' ;
PERCENT   : '%' ;
PLUS      : '+' ;
MINUS     : '-' ;


/* Whitespace - we skip that. */

NEWLINE : ( ('\r')? '\n' ) { newline(); $setType(Token.SKIP); };
WS      : ( ' ' | '\t' )+ { $setType(Token.SKIP); };

/**
 * Comments - we skip those too.
 *
 * Note:  No need to mention '\r' in this rule since it will match the wildcard
 *        character that consumes characters up to the '\n'.
 */
COMMENT : ( '-' '-' ( options { greedy = false; } : . )* '\n' )
          { newline(); $setType(Token.SKIP); };

/* Comparison Operators */

COMPARE_OPERATOR :
    '=' ('=')? { $setType(EQUALS); }
  | '<' { $setType(LESS_THAN); }
    ( ( '>' { $setType(NOT_EQUALS); } ) | ( '=' { $setType(LESS_EQUAL ); } ) )?
  | '!' '=' { $setType(NOT_EQUALS); }
  | '>' { $setType(GRTR_THAN); }
    ( '=' { $setType(GRTR_EQUAL); } )?
  ;


/* Identifiers */

// SQL identifiers are "rolled up" to upper-case characters.
IDENT options { testLiterals = true; } :
  ('A'..'Z' | 'a'..'z' | '_') ('A'..'Z' | 'a'..'z' | '0'..'9' | '_')*
  { String su = $getText.toUpperCase(); $setText(su); }
  ;

// SQL identifiers can be double-quoted.  This preserves the case of the
// identifier, and allows keywords to be used as identifiers.  ("UGH," I know...)
QUOTED_IDENT :
  '"'! ('A'..'Z' | 'a'..'z' | '_') ('A'..'Z' | 'a'..'z' | '0'..'9' | '_')* '"'! ;


/* Literals of various types */

/**
 * Number-literal parsing is tricky, because you might have an integer, a
 * decimal number, or a simple period ('.') by itself.  This lexer rule handles
 * all three of these options, and sets the token-type appropriately.
 * <p>
 * Note that these numbers are <i>unsigned</i>.  Signed numbers have to be
 * processed separately.
 */
NUM_LITERAL_OR_SYMBOL :
    ('0'..'9')+ { $setType(INT_LITERAL); }
      ( ('L'! { $setType(LONG_LITERAL); } )
      | ('.' { $setType(DEC_LITERAL); } ('0'..'9')*
          ( ('f'! | 'F'!) { $setType(FLOAT_LITERAL); } )?
        )
      )?
  | '.' { $setType(PERIOD); }
      ( ('0'..'9') { $setType(DEC_LITERAL); } ('0'..'9')*
        ( ('f'! | 'F'!) { $setType(FLOAT_LITERAL); } )?
      )?
  ;

STRING_LITERAL : '\''! ( ~( '\'' | '\r' | '\n' ))* '\''! ;
