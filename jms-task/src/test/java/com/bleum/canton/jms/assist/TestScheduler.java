package com.bleum.canton.jms.assist;

import com.bleum.canton.jms.entity.JMSTask;
import com.bleum.canton.jms.scheduler.AbstractJMSScheduler;

/**
 * 
 * Description: Stub Scheduler for unit test/
 * 
 * @author jay.li
 * @created Feb 25, 2014 3:45:46 PM
 * 
 */
public class TestScheduler extends AbstractJMSScheduler {

    final static public int TEST_TYPE = 88;

    public TestScheduler() {
        super();
        setTaskType(TEST_TYPE);
    }

    @Override
    protected void sendMessage(final JMSTask task) {

        if (task.getMessage().equals("normal")
                || task.getMessage().equals("processed")) {
            // success
        } else if (task.getMessage().equals("error")) {
            if (task.getRetry() < 2) {
                throw new RuntimeException("test error");
            } else {
                // success
            }
        } else if (task.getMessage().equals("fatal")) {
            throw new RuntimeException("test fatal");
        }
    }

    @Override
    protected String formMessage(JMSTask task) {
        // TODO Auto-generated method stub
        return null;
    }

}