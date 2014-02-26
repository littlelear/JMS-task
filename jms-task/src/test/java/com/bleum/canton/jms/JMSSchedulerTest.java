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
 * Description: Unit test for JMS scheduler with no acknowledge.
 * 
 * @author jay.li
 * @created Feb 25, 2014 3:47:54 PM
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:com/bleum/canton/jms-test.xml" })
public class JMSSchedulerTest {

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
        testScheduler.setMaxTasksPerThread(taskPerT);
        testScheduler.setThreads(thread);
        testScheduler.setClientAck(JMSTaskConstant.NO_ACKNOWLEDGE);

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
                TestScheduler.TEST_TYPE, JMSTaskConstant.COMPLETED);
        assertEquals(nTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(taskPerT * thread - nTask, l.size());

        // second run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 12 tasks are handled, still 6 are processed, the reset 12 are error
        // (3 retry set to 1, 9 retry set to 2)
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(eTask + fTask, l.size());

        // third run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 12 tasks are handled, 6 error tasks are processed, still 6 are
        // already processed, the reset 6 are still error (3 retry set to 2, 3
        // retry set to 3)
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(fTask, l.size());

        // fourth run
        testScheduler.run();

        // wait to finish processing
        runFutures = testScheduler.getRunFutures();
        for (Future<?> future : runFutures) {
            future.get();
        }

        // 6 tasks are handled, 12 are already processed, these 3 are fatal, the
        // reset 6 are still error (retry set to 3)
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
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

        // 3 tasks are handled, 12 are already processed, 6 are fatal
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.COMPLETED);
        assertEquals(nTask + eTask, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.ERROR);
        assertEquals(0, l.size());
        l = jmsTestUtils.findTaskByTypeAndStatus(TestScheduler.TEST_TYPE,
                JMSTaskConstant.FATAL);
        assertEquals(fTask, l.size());
    }

}
