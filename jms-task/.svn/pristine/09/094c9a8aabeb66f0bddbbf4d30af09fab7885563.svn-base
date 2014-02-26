package com.bleum.canton.jms;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bleum.canton.jms.assist.JMSTestUtils;
import com.bleum.canton.jms.constant.JMSTaskConstant;
import com.bleum.canton.jms.dao.IJMSTaskDao;
import com.bleum.canton.jms.entity.JMSTask;

/**
 * 
 * Description: Unit test for JMSTaskDao.
 * 
 * @author jay.li
 * @created Feb 25, 2014 3:48:04 PM
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:com/bleum/canton/jms-test.xml" })
public class JMSTaskDAOTest {

    @Resource(name = "jmsTestUtils")
    private JMSTestUtils jmsTestUtils;

    @Resource(name = "jmsTaskDao")
    private IJMSTaskDao jmsTaskDao;

    @Test
    public void addTask() {

        JMSTask task = new JMSTask();
        task.setCreatedDate(new Date());
        task.setOperatedDate(new Date());
        task.setType(JMSTaskConstant.INVENTORY_UPDATE);
        task.setStatus(JMSTaskConstant.NEW);
        task.setRetry(0);
        StringBuilder sb = new StringBuilder();
        sb.append("<InventoryUpdate>");
        sb.append("xyZ");
        sb.append("</InventoryUpdate>");
        task.setMessage(sb.toString());

        jmsTaskDao.addTask(task);

        // id start from 0 by auto increment;
        int id = jmsTestUtils.lastId();
        JMSTask result = jmsTestUtils.getTaskById(id);

        assertEquals(id, result.getId());
        assertEquals(task.getType(), result.getType());
        assertEquals(task.getStatus(), result.getStatus());
        assertEquals(task.getMessage(), result.getMessage());

    }

    @Test
    public void updateTaskForProcessing() {
        jmsTaskDao.updateTaskForProcessing(JMSTaskConstant.ORDER_CREATE, 5);

        JMSTask result = jmsTestUtils.getTaskById(1);
        assertEquals(JMSTaskConstant.PROCESSING, result.getStatus());
        result = jmsTestUtils.getTaskById(2);
        assertEquals(JMSTaskConstant.PROCESSING, result.getStatus());
        result = jmsTestUtils.getTaskById(3);
        assertEquals(JMSTaskConstant.PROCESSING, result.getStatus());
        result = jmsTestUtils.getTaskById(4);
        assertEquals(JMSTaskConstant.PROCESSING, result.getStatus());
        result = jmsTestUtils.getTaskById(5);
        assertEquals(JMSTaskConstant.PROCESSING, result.getStatus());
        result = jmsTestUtils.getTaskById(6);
        assertEquals(JMSTaskConstant.NEW, result.getStatus());
    }

    @Test
    public void findTaskForProcessing() {
        List<JMSTask> l = jmsTaskDao.findTaskForProcessing(3, 3);

        assertEquals(JMSTaskConstant.PROCESSING, l.get(0).getStatus());

        assertEquals(JMSTaskConstant.PROCESSING, l.get(1).getStatus());

        assertEquals(JMSTaskConstant.PROCESSING, l.get(2).getStatus());
    }

    @Test
    public void updateTaskStatusByTaskId() {

        JMSTask result = jmsTestUtils.getTaskById(2001);
        assertEquals(2, result.getStatus());
        result = jmsTestUtils.getTaskById(2002);
        assertEquals(3, result.getStatus());

        jmsTaskDao.updateTaskStatusByTaskId(2001, 10);
        jmsTaskDao.updateTaskStatusByTaskId(2002, 10);

        result = jmsTestUtils.getTaskById(2001);
        assertEquals(10, result.getStatus());
        result = jmsTestUtils.getTaskById(2002);
        assertEquals(10, result.getStatus());
    }

    @Test
    public void updateProcessedTask1() {

        JMSTask result = jmsTestUtils.getTaskById(2101);
        assertEquals(100, result.getStatus());

        JMSTask task = new JMSTask();
        task.setId(2101);
        task.setRetry(2);

        jmsTaskDao.updateTaskProcessed(task, 2);

        result = jmsTestUtils.getTaskById(2101);
        assertEquals(JMSTaskConstant.COMPLETED, result.getStatus());
    }

    @Test
    public void updateProcessedTask2() {

        JMSTask result = jmsTestUtils.getTaskById(2102);
        assertEquals(100, result.getStatus());

        JMSTask task = new JMSTask();
        task.setId(2102);
        task.setRetry(2);

        jmsTaskDao.updateTaskProcessed(task, 3);

        result = jmsTestUtils.getTaskById(2102);
        assertEquals(JMSTaskConstant.PROCESSED, result.getStatus());
        assertEquals(3, result.getRetry());
    }

    @Test
    public void updateErrorTask1() {

        JMSTask result = jmsTestUtils.getTaskById(3001);
        assertEquals(100, result.getStatus());

        JMSTask task = new JMSTask();
        task.setId(3001);
        task.setLastError("test error");
        task.setRetry(2);

        jmsTaskDao.updateErrorTask(task, 2);

        result = jmsTestUtils.getTaskById(3001);
        assertEquals(JMSTaskConstant.FATAL, result.getStatus());
        assertEquals("test error", result.getLastError());
    }

    @Test
    public void updateErrorTask2() {

        JMSTask result = jmsTestUtils.getTaskById(3002);
        assertEquals(99, result.getStatus());

        JMSTask task = new JMSTask();
        task.setId(3002);
        task.setLastError("test error");
        task.setRetry(2);

        jmsTaskDao.updateErrorTask(task, 3);

        result = jmsTestUtils.getTaskById(3002);
        assertEquals(JMSTaskConstant.ERROR, result.getStatus());
        assertEquals(3, result.getRetry());
        assertEquals("test error", result.getLastError());
    }

    @Test
    public void updateTaskCompeleted() {

        JMSTask result = jmsTestUtils.getTaskById(4001);
        assertEquals(99, result.getStatus());

        jmsTaskDao.updateTaskCompeleted(4001);

        result = jmsTestUtils.getTaskById(4001);
        assertEquals(JMSTaskConstant.COMPLETED, result.getStatus());
    }

    @Test
    public void updateTaskFatal() {

        JMSTask result = jmsTestUtils.getTaskById(5001);
        assertEquals("error1", result.getLastError());

        jmsTaskDao.updateTaskFatal(5001, "test error");

        result = jmsTestUtils.getTaskById(5001);
        assertEquals("test error", result.getLastError());
    }
}
