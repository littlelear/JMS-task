package com.bleum.canton.jms;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bleum.canton.jms.assist.JMSTestUtils;
import com.bleum.canton.jms.assist.TestScheduler;
import com.bleum.canton.jms.constant.JMSTaskConstant;
import com.bleum.canton.jms.dao.IJMSTaskDao;
import com.bleum.canton.jms.entity.JMSTask;
import com.bleum.canton.jms.scheduler.AbstractJMSScheduler;

/**
 * 
 * Description: Unit test for JMS scheduler with client acknowledge.
 * 
 * @author jay.li
 * @created Feb 25, 2014 3:47:00 PM
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:com/bleum/canton/jms-test.xml" })
public class JMSSchedulerClientAckTest {

    private int nTask = 6;
    private int eTask = 6;
    private int fTask = 6;
    private int retry = 3;
    private int taskPerT = 5;
    private int thread = 3;

    @Resource(name = "jmsTestUtils")
    private JMSTestUtils jmsTestUtils;

    @Resource(name = "jmsTaskDao")
    private IJMSTaskDao jmsTaskDao;

    private AbstractJMSScheduler testScheduler = new TestScheduler();

    @Before
    public void setup() throws IllegalArgumentException, SecurityException,
            IllegalAccessException, NoSuchFieldException {
        testScheduler.setMaxRetry(retry);
        testScheduler.setMaxAckRetry(retry);
        testScheduler.setMaxTasksPerThread(taskPerT);
        testScheduler.setThreads(thread);
        testScheduler.setClientAck(JMSTaskConstant.CLIENT_ACKNOWLEDGE);

        Field dao = testScheduler.getClass().getSuperclass()
                .getDeclaredField("jmsTaskDao");
        dao.setAccessible(true);
        dao.set(testScheduler, jmsTaskDao);

        JMSTask task = new JMSTask();
        task.setCreatedDate(new Date());
        task.setOperatedDate(new Date());
        task.setType(TestScheduler.TEST_TYPE);
        task.setStatus(JMSTaskConstant.NEW);
        task.setRetry(0);
        task.setMessage("normal");
        for (int i = 0; i < nTask; i++) {
            jmsTaskDao.addTask(task);
        }
        task.setMessage("error");
        for (int i = 0; i < eTask; i++) {
            jmsTaskDao.addTask(task);
        }
        task.setMessage("fatal");
        for (int i = 0; i < fTask; i++) {
            jmsTaskDao.addTask(task);
        }
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        // first run
        testScheduler.run();

        // wait to finish processing
        Collection<Future<?>> runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 15 tasks are handled, 6 are processed, the reset are error (retry set
        // to 1)
        List<JMSTask> l = jmsTestUtils.findTaskByTypeAndStatus(
                TestScheduler.TEST_TYPE, JMSTaskConstant.PROCESSED);
        assertEquals(nTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(taskPerT * thread - nTask, l.size());

        // no acknowledge

        // second run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 6 tasks are processed again (retry 1), the reset 9 are error
        // (9 retry set to 2)
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);
        assertEquals(nTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(taskPerT * thread - nTask, l.size());

        // acknowledge 3 tasks
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);

        int ackNum = 3;
        jmsTaskDao.updateTaskCompeleted(l.get(0).getId());
        jmsTaskDao.updateTaskCompeleted(l.get(1).getId());
        jmsTaskDao.updateTaskCompeleted(l.get(2).getId());

        // third run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 3 are processed to completed, 3 are still processed (retry 2),
        // 6 error tasks are processed the reset 6 are still error (3 retry set
        // to 1, 3 retry set to 3)
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(ackNum, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);
        assertEquals(nTask + eTask - ackNum, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(fTask, l.size());

        // no acknowledge

        // fourth run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 3 are completed (retry 3 turn to completed),
        // 6 error tasks were processed last round , changed to completed (retry
        // 3 turn to completed)
        // 0 processed
        // 3 error (retry 2), 3 fatals
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);
        assertEquals(0, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        int second_batch = nTask + eTask + fTask - taskPerT * thread;
        assertEquals(second_batch, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.FATAL);
        assertEquals(fTask - second_batch, l.size());

        // fifth run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 3 error (retry 3), 3 fatals
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);
        assertEquals(0, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(second_batch, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.FATAL);
        assertEquals(fTask - second_batch, l.size());

        // sixth run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 6 fatals
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.PROCESSED);
        assertEquals(0, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(0, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.FATAL);
        assertEquals(fTask, l.size());

    }

}
