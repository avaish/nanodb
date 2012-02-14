package edu.caltech.nanodb.qeval;


public class PlannerFactory {
    /**
     * This property can be used to specify a different query-planner class
     * for NanoDB to use.
     */
    public static final String PROP_PLANNER_CLASS = "nanodb.planner.class";


    /**
     * This class is the default planner used in NanoDB, unless
     * overridden in the configuration.
     */
    public static final String DEFAULT_PLANNER =
        "edu.caltech.nanodb.qeval.DPJoinPlanner";


    private PlannerFactory() {
        throw new UnsupportedOperationException(
            "This class should not be instantiated.");
    }


    public static Planner getPlanner() {
        String className =
            System.getProperty(PROP_PLANNER_CLASS, DEFAULT_PLANNER);

        try {
            // Load and instantiate the specified planner class.
            Class c = Class.forName(className);
            Planner p = (Planner) c.newInstance();
            return p;
        }
        catch (Exception e) {
            throw new RuntimeException(
                "Couldn't instantiate Planner class " + className, e);
        }
    }
}

