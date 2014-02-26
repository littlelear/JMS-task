package com.bleum.canton.jms.constant;

/**
 * 
 * Description: Constant for JMS Platform.
 * 
 * @author jay.li
 * @created Feb 19, 2014 3:16:38 PM
 * 
 */
public final class JMSTaskConstant {

    private JMSTaskConstant() {
    }

    // Task Status
    public static final int NEW = 1;
    public static final int PROCESSING = 2;
    public static final int PROCESSED = 3;
    public static final int COMPLETED = 4;
    public static final int ERROR = 5;
    public static final int FATAL = 6;

    // Task Type
    public static final int ORDER_CREATE = 1;
    public static final int INVENTORY_UPDATE = 2;

    // Scheduler Default
    // 5 minutes
    public static final long DEFAULT_FIXED_DELAY = 300000l;

    // 5 minutes
    public static final long DEFAULT_INITIAL_DELAY = 300000l;

    // thread
    public static final int DEFAULT_MAX_THREADS = 3;
    public static final int DEFAULT_MAX_TASKS_PER_THREADS = 100;
    public static final int DEFAULT_MAX_RETRY = 3;

    // Client Acknowledge Type
    public static final int NO_ACKNOWLEDGE = 1;
    public static final int CLIENT_ACKNOWLEDGE = 2;

}
