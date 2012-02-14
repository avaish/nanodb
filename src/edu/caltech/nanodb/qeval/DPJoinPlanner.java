package edu.caltech.nanodb.qeval;


import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;

import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import org.apache.log4j.Logger;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations don't
 * currently span multiple subqueries.
 */
public class DPJoinPlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DPJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in the
     * <tt>FROM</tt>-clause of the query.  However, the planner will attempt to
     * push conjuncts down the plan as far as possible, so even if a leaf is a
     * base table, the plan may be a bit more complex than just a single
     * file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not be
         * used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to this
         *        leaf, or it may be nonempty if some conjuncts apply solely to
         *        this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause) throws IOException {

        // We want to take a simple SELECT a, b, ... FROM A, B, ... WHERE ...
        // and turn it into a tree of plan nodes.

        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            throw new UnsupportedOperationException(
                "NanoDB doesn't yet support SQL queries without a FROM clause!");
        }

        // Pull out the top-level conjuncts from the WHERE clause on the query,
        // since we will handle them in special ways if we have outer joins.

        HashSet<Expression> whereConjuncts = new HashSet<Expression>();
        addConjuncts(whereConjuncts, selClause.getWhereExpr());

        // Create an optimal join plan from the top-level from-clause and the
        // top-level conjuncts.
        JoinComponent joinComp = makeJoinPlan(fromClause, whereConjuncts);
        PlanNode plan = joinComp.joinPlan;

        // If there are any left-over conjuncts we didn't use, apply them here
        HashSet<Expression> unusedConjuncts =
            new HashSet<Expression>(whereConjuncts);
        unusedConjuncts.removeAll(joinComp.conjunctsUsed);

        Expression finalPredicate = makePredicate(unusedConjuncts);
        if (finalPredicate != null)
            plan = addPredicateToPlan(plan, finalPredicate);

        // Grouping/aggregation will go somewhere in here.

        // Depending on the SELECT clause, create a project node at the top of
        // the tree.
        if (!selClause.isTrivialProject()) {
            List<SelectValue> selectValues = selClause.getSelectValues();
            plan = new ProjectNode(plan, selectValues);
        }

        // Finally, apply any sorting at the end.
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (!orderByExprs.isEmpty())
            plan = new SortNode(plan, orderByExprs);

        plan.prepare();

        return plan;
    }


    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        conjuncts.addAll(extraConjuncts);
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");

        // Pass an unmodifiable set of the input conjuncts, to keep bugs from
        // happening...
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization out
     * of a <tt>FROM</tt> clause.
     * <p>
     * We accumulate conjucts and leaves from a from clause (and children from
     * clauses that are not leaves). A leaf is defined to be a from clause
     * representing a base table, a subquery, or an outer join. Conjucts are
     * collected from from clause predicates of non leaf from clauses.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {
        if (fromClause.isJoinExpr() && !fromClause.isOuterJoin()) {
            addConjuncts(conjuncts, fromClause.getPreparedJoinExpr());
            
            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
        }
        else {
            leafFromClauses.add(fromClause);
        }
    }


    /**
     * This helper method takes a predicate <tt>expr</tt> and stores all of its
     * conjuncts into the specified collection of conjuncts.  Specifically, if
     * the predicate is a Boolean <tt>AND</tt> operation then each term will
     * individually be added to the collection of conjuncts.  Any other kind of
     * predicate will be stored as-is into the collection.
     *
     * @param conjuncts the collection of conjuncts to add the predicate (or its
     *        components) to.
     *
     * @param expr the expression to pull the conjuncts out of
     */
    private void addConjuncts(Collection<Expression> conjuncts, Expression expr) {
        // If there is no condition, just return without doing anything.
        if (expr == null)
            return;

        // If it's an AND expression, add the terms to the set of conjuncts.
        if (expr instanceof BooleanOperator) {
            BooleanOperator boolExpr = (BooleanOperator) expr;
            if (boolExpr.getType() == BooleanOperator.Type.AND_EXPR) {
                for (int iTerm = 0; iTerm < boolExpr.getNumTerms(); iTerm++)
                    conjuncts.add(boolExpr.getTerm(iTerm));
            }
            else {
                // The Boolean expression is an OR or NOT, so we can't add the
                // terms themselves.
                conjuncts.add(expr);
            }
        }
        else {
            // The predicate is not a Boolean expression, so just store it.
            conjuncts.add(expr);
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<Expression>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * <p>
     * For a given leaf from clause, we push down as many conjuncts as possible
     * (those that only apply to that leaf) and determine the most optimal 
     * plan for the leaf from clause (file scans for base tables, recursive
     * calls for subqueries, and a nested loop for an outer join.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts the conjuncts available to apply in this leaf-plan
     *
     * @param leafConjuncts This is an out-parameter that specifies a
     *        collection where any conjuncts used in constructing the
     *        leaf-plan should be added to.  This is how the planner can tell
     *        what conjuncts still need to be applied when constructing the
     *        parts of the plan above the leaf.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {
        
        PlanNode top = null;
        
        if (fromClause.isBaseTable()) {
            top = makeSimpleSelect(fromClause.getTableName(), null);
            if (fromClause.isRenamed())
                top = new RenameNode(top, fromClause.getResultName());
            
            top.prepare();
            
            // Find conjuncts that might apply to this leaf.
            findExprsUsingSchemas(conjuncts, false, leafConjuncts, 
                top.getSchema());
            
            // If we have conjuncts to pass down, do so.
            if (!leafConjuncts.isEmpty()) {
                addPredicateToPlan(top, makePredicate(leafConjuncts));
                top.prepare();
            }
        }
        else if (fromClause.isDerivedTable()) {
            top = new RenameNode((makePlan(fromClause.getSelectClause())), 
                fromClause.getResultName());
            
            top.prepare();
            
            // Find conjuncts that might apply to this leaf.
            findExprsUsingSchemas(conjuncts, false, leafConjuncts, 
                top.getSchema());
            
            // If we have conjuncts to pass down, do so.
            if (!leafConjuncts.isEmpty()) {
                addPredicateToPlan(top, makePredicate(leafConjuncts));
                top.prepare();
            }
        }
        else if (fromClause.isOuterJoin()) {
            JoinComponent left_plan;
            JoinComponent right_plan;
            
            // If we have no outer join on right, the left child can take in
            // conjuncts without a change in overall results.
            if (!fromClause.hasOuterJoinOnRight()) {
                left_plan = makeJoinPlan(fromClause.getLeftChild(), conjuncts);
                leafConjuncts = left_plan.conjunctsUsed;
            }
            else {
                left_plan = makeJoinPlan(fromClause.getLeftChild(), 
                    new HashSet<Expression>());
            }
            
            // If we have no outer join on left, the right child can take in
            // conjuncts without a change in overall results.
            if (!fromClause.hasOuterJoinOnLeft()) {
                right_plan = makeJoinPlan(fromClause.getRightChild(), conjuncts);
                leafConjuncts = right_plan.conjunctsUsed;
            }
            else {
                right_plan = makeJoinPlan(fromClause.getLeftChild(), 
                    new HashSet<Expression>());
            }
            
            PlanNode left = left_plan.joinPlan;
            PlanNode right = right_plan.joinPlan;
            
            top = new NestedLoopsJoinNode(left, right, fromClause.getJoinType(), 
                fromClause.getPreparedJoinExpr());
            
            top.prepare();
        }
        else {
            throw new IllegalArgumentException("Invalid from clause.");
        }
        
        return top;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate!  Plans only get
            // stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();

            for (HashSet<PlanNode> nodes : joinPlans.keySet()) {
                JoinComponent plan_n = joinPlans.get(nodes);
                
                for (JoinComponent leaf : leafComponents) {
                    // If the plan already contains this leaf, continue.
                    if (nodes.containsAll(leaf.leavesUsed)) continue;
                    
                    // Get a set of conjuncts used in the leaf and the current
                    // plan, to determine unused conjuncts.
                    HashSet<Expression> conjunctsUsed = new HashSet<Expression>();
                    conjunctsUsed.addAll(leaf.conjunctsUsed);
                    conjunctsUsed.addAll(plan_n.conjunctsUsed);
                    
                    HashSet<Expression> unusedConjuncts = 
                        new HashSet<Expression>(conjuncts);
                    
                    unusedConjuncts.removeAll(conjunctsUsed);
                    HashSet<Expression> appliedConjuncts = 
                        new HashSet<Expression>();
                    
                    // Find all the conjuncts that are unused and can be applied
                    // to the joined schema of both the current plan and the leaf.
                    findExprsUsingSchemas(unusedConjuncts, false, appliedConjuncts,
                        plan_n.joinPlan.getSchema(), leaf.joinPlan.getSchema());
                    
                    conjunctsUsed.addAll(appliedConjuncts);
                    
                    // Create the new Plan Node.
                    PlanNode new_node = new NestedLoopsJoinNode(plan_n.joinPlan, 
                        leaf.joinPlan, JoinType.INNER, 
                        makePredicate(appliedConjuncts));
                    
                    // Prepare and cost the new node. This ensures we don't have
                    // to prepare above.
                    new_node.prepare();
                    float cost = new_node.getCost().cpuCost;
                    
                    HashSet<PlanNode> new_leaves = new HashSet<PlanNode>(nodes);
                    new_leaves.addAll(leaf.leavesUsed);
                    
                    JoinComponent new_plan = new JoinComponent(new_node, 
                        new_leaves, conjunctsUsed);
                    
                    // We want to make sure only the lowest cost plan is stored
                    // for any collection of leaves.
                    if (nextJoinPlans.containsKey(new_leaves)) {
                        if (cost < nextJoinPlans.get(new_leaves).joinPlan.
                            getCost().cpuCost) {
                            nextJoinPlans.put(new_leaves, new_plan);
                        }
                    }
                    else {
                        nextJoinPlans.put(new_leaves, new_plan);
                    }
                }
            }

            // Now that we have generated all plans joining n leaves, time to
            // create all plans joining n+1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
    }


    /**
     * This helper function takes a collection of conjuncts that should comprise
     * a predicate, and creates a predicate for evaluating these conjuncts.  The
     * exact nature of the predicate depends on the conjuncts:
     * <ul>
     *   <li>If the collection contains only one conjunct, the method simply
     *       returns that one conjunct.</li>
     *   <li>If the collection contains two or more conjuncts, the method
     *       returns a {@link BooleanOperator} that performs an <tt>AND</tt> of
     *       all conjuncts.</li>
     *   <li>If the collection contains <em>no</em> conjuncts then the method
     *       returns <tt>null</tt>.
     * </ul>
     *
     * @param conjuncts the collection of conjuncts to combine into a predicate.
     *
     * @return a predicate for evaluating the conjuncts, or <tt>null</tt> if the
     *         input collection contained no conjuncts.
     */
    private Expression makePredicate(Collection<Expression> conjuncts) {
        Expression predicate = null;
        if (conjuncts.size() == 1) {
            predicate = conjuncts.iterator().next();
        }
        else if (conjuncts.size() > 1) {
            predicate = new BooleanOperator(
                BooleanOperator.Type.AND_EXPR, conjuncts);
        }
        return predicate;
    }


    /**
     * This helper function takes a query plan and a selection predicate, and
     * adds the predicate to the plan in a reasonably intelligent way.
     * <p>
     * If the plan is a subclass of the {@link SelectNode} then the select
     * node's predicate is updated to include the predicate.  Specifically, if
     * the select node already has a predicate then one of the following occurs:
     * <ul>
     *   <li>If the select node currently has no predicate, the new predicate is
     *       assigned to the select node.</li>
     *   <li>If the select node has a predicate whose top node is a
     *       {@link BooleanOperator} of type <tt>AND</tt>, this predicate is
     *       added as a new term on that node.</li>
     *   <li>If the select node has some other kind of non-<tt>null</tt>
     *       predicate then this method creates a new top-level <tt>AND</tt>
     *       operation that will combine the two predicates into one.</li>
     * </ul>
     * <p>
     * If the plan is <em>not</em> a subclass of the {@link SelectNode} then a
     * new {@link SimpleFilterNode} is added above the current plan node, with
     * the specified predicate.
     *
     * @param plan the plan to add the selection predicate to
     *
     * @param predicate the selection predicate to add to the plan
     * 
     * @return the (possibly new) top plan-node for the plan with the selection
     *         predicate applied
     */
    private PlanNode addPredicateToPlan(PlanNode plan, Expression predicate) {
        if (plan instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) plan;

            if (selectNode.predicate != null) {
                // There is already an existing predicate.  Add this as a
                // conjunct to the existing predicate.
                Expression fsPred = selectNode.predicate;
                boolean handled = false;

                // If the current predicate is an AND operation, just make
                // the where-expression an additional term.
                if (fsPred instanceof BooleanOperator) {
                    BooleanOperator bool = (BooleanOperator) fsPred;
                    if (bool.getType() == BooleanOperator.Type.AND_EXPR) {
                        bool.addTerm(predicate);
                        handled = true;
                    }
                }

                if (!handled) {
                    // Oops, the current file-scan predicate wasn't an AND.
                    // Create an AND expression instead.
                    BooleanOperator bool =
                        new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                    bool.addTerm(fsPred);
                    bool.addTerm(predicate);
                    selectNode.predicate = bool;
                }
            }
            else {
                // Simple - just add where-expression onto the file-scan.
                selectNode.predicate = predicate;
            }
        }
        else {
            // The subplan is more complex, so put a filter node above it.
            plan = new SimpleFilterNode(plan, predicate);
        }

        return plan;
    }


    /**
     * This helper method takes a collection of expressions, and finds those
     * expressions that can be evaluated solely against the provided set of one
     * or more schemas.  In other words, if an expression doesn't refer to any
     * symbols outside of the specified set of schemas, then it will be included
     * in the result collection.
     * <p>
     * The last argument to this method is one or more {@link Schema} objects to
     * check expressions against.  For example, we can perform operations like
     * this:
     * <pre>
     *   Schema s1 = ...;
     *   Schema s2 = ...;
     *
     *   // Find expressions in srcExprs that can be evaluated against s1, and
     *   // add them to dstExprs.  Do not remove matching exprs from srcExprs.
     *   findExprsUsingSchemas(srcExprs, false, dstExprs, s1);
     *
     *   // Find expressions in srcExprs that can be evaluated against the
     *   // combination of s1 and s2, and add them to dstExprs.  Remove
     *   // matching exprs from srcExprs.
     *   findExprsUsingSchemas(srcExprs, true, dstExprs, s1, s2);
     * </pre>
     *
     * @param srcExprs the input collection of expressions to check against the
     *        provided schemas.
     *
     * @param remove if <tt>true</tt>, the matching expressions will be removed
     *        from the <tt>srcExprs</tt> collection.  Otherwise, the
     *        <tt>srcExprs</tt> collection is left unchanged.
     *
     * @param dstExprs the collection to add the matching expressions to.  This
     *        collection is <tt>not</tt> cleared by this method; any previous
     *        contents in the collection will be left unchanged.
     *
     * @param schemas a collection of one or more schemas to check the input
     *        expressions against.  If an expression can be evaluated solely
     *        against these schemas then it will be added to the results.
     */
    public static void findExprsUsingSchemas(Collection<Expression> srcExprs,
        boolean remove, Collection<Expression> dstExprs, Schema... schemas) {

        ArrayList<ColumnName> symbols = new ArrayList<ColumnName>();

        Iterator<Expression> termIter = srcExprs.iterator();
        while (termIter.hasNext()) {
            Expression term = termIter.next();

            // Read all symbols from this term.
            symbols.clear();
            term.getAllSymbols(symbols);

            // If *all* of the symbols in the term reference at least one of the
            // provided schemas, add it to the results (removing from this
            // operator, if so directed by caller).
            boolean allRef = true;
            for (ColumnName colName : symbols) {
                // Determine if *this* symbol references at least one schema.
                boolean ref = false;
                for (Schema schema : schemas) {
                    if (schema.getColumnIndex(colName) != -1) {
                        ref = true;
                        break;
                    }
                }

                // If this symbol doesn't reference any of the schemas then
                // this term doesn't qualify.
                if (!ref) {
                    allRef = false;
                    break;
                }
            }

            if (allRef) {
                dstExprs.add(term);
                if (remove)
                    termIter.remove();
            }
        }
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or <tt>null</tt> if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName,
        Expression predicate) throws IOException {

        // Open the table.
        TableFileInfo tableInfo = StorageManager.getInstance().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        return new FileScanNode(tableInfo, predicate);
    }
}
