/* <Bleum Inc. Copyright>
 * Copyright (C) 2013 Bleum Inc.
 * All Rights Reserved.  No use, copying or distribution of this
 * work may be made except in accordance with a valid license
 * agreement from Bleum Inc..  This notice must be
 * included on all copies, modifications and derivatives of this
 * work.
 *
 * Bleum Inc. MAKES NO REPRESENTATIONS OR WARRANTIES
 * ABOUT THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. Belum Inc. 
 * SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF 
 * USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * </Bleum Inc. Copyright>
 */
package com.bleum.canton.jms.scheduler;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.bleum.canton.jms.constant.JMSTaskConstant;
import com.bleum.canton.jms.dao.IJMSTaskDao;
import com.bleum.canton.jms.entity.JMSTask;

/**
 * 
 * Description: JMS scheduler can run by fixed delay, fix rate and cron. when
 * it's triggered, it collects the related tasks, and sends them by configured
 * threads.
 * 
 * @author jay.li
 * @created Feb 19, 2014 10:30:44 AM
 * 
 */
public abstract class AbstractJMSScheduler implements Runnable {

    /**
     * taks type
     */
    private int taskType;

    /**
     * fixed delay milliseconds
     */
    private long fixedDelay;

    /**
     * fixed rate milliseconds, if fixedDelay is set, this will be ignored.
     */
    private long fixedRate;

    /**
     * cron, if fixedDelay or fixedRate is set, this will be ignored.
     */
    private String cron;

    /**
     * Initial delay, after which the scheduler is triggered.
     */
    private long initialDelay;

    /**
     * Max allowed thread number.
     */
    private int threads;

    /**
     * Max retry.
     */
    private int maxRetry;

    /**
     * Max acknowledge retry.
     */
    private int maxAckRetry;

    /**
     * Max tasks one thread can do.
     */
    private int maxTasksPerThread;

    /**
     * Client acknowledge type
     */
    private int clientAck;

    /**
     * Spring wrapper for jdk scheduledExecutor
     */
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @Resource(name = "jmsTaskDao")
    private IJMSTaskDao jmsTaskDao;

    /**
     * Using these future can help monitoring results of task threads
     */
    private Collection<Future<?>> runFutures;

    /**
     * JMS sender. If don't send jms, don't set this field and override
     * <tt>sendMessage</tt>
     */
    private JmsTemplate jmsSender;

    /**
     * Reply queue for client acknowledge.
     */
    private Queue replyQueue;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        // re-initialize the futures
        runFutures = new LinkedList<Future<?>>();

        // load tasks
        List<JMSTask> tasks = loadTasks();

        // execute tasks
        excuteTasks(tasks);

        // Wait for all tasks done. If quit immediately, there's a chance next
        // scheduler runs, and will cause status conflict. FixedDelay and
        // fixedRate guarantee Spring scheduler run after the previous one.
        for (Future<?> future : runFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                // no need to handle
            } catch (ExecutionException e) {
                // no need to handle
            }
        }
    }

    /**
     * For the XML message fro JMS sending.
     * 
     * @param task
     * @return
     */
    abstract protected String formMessage(JMSTask task);

    /**
     * Send JMS message. Can be override.
     * 
     * @param task
     */
    protected void sendMessage(final JMSTask task) {
        if (jmsSender == null) {
            throw new RuntimeException("jmsSender is null.");
        }
        MessageCreator mc = new MessageCreator() {

            public Message createMessage(Session session) throws JMSException {
                TextMessage om = session.createTextMessage();
                om.setText(formMessage(task));
                om.setIntProperty("clientAck", clientAck);
                if (clientAck == JMSTaskConstant.CLIENT_ACKNOWLEDGE) {
                    om.setLongProperty("taskId", task.getId());
                    om.setJMSReplyTo(replyQueue);
                }
                return om;
            }
        };
        jmsSender.send(mc);
    }

    /**
     * Auto start the scheduler after all properties set.
     */
    @PostConstruct
    private void start() {
        // initialize Spring ThreadPoolTaskScheduler
        threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.initialize();

        // set properties
        setProperties();

        // Spring ThreadPoolTaskScheduler
        Date startDate = new Date(System.currentTimeMillis() + initialDelay);
        if (fixedDelay > 0) {
            threadPoolTaskScheduler.scheduleWithFixedDelay(this, startDate,
                    fixedDelay);
        } else if (fixedRate > 0) {
            threadPoolTaskScheduler.scheduleAtFixedRate(this, startDate,
                    fixedDelay);
        } else if (StringUtils.isNotBlank(cron)) {
            threadPoolTaskScheduler.schedule(this, new CronTrigger(cron));
        } else {
            threadPoolTaskScheduler.scheduleWithFixedDelay(this, startDate,
                    JMSTaskConstant.DEFAULT_FIXED_DELAY);
        }
    }

    /**
     * Set properties
     */
    private void setProperties() {
        if (threads <= 0) {
            threads = JMSTaskConstant.DEFAULT_MAX_THREADS;
        }
        if (maxTasksPerThread <= 0) {
            maxTasksPerThread = JMSTaskConstant.DEFAULT_MAX_TASKS_PER_THREADS;
        }
        if (initialDelay <= 0) {
            initialDelay = JMSTaskConstant.DEFAULT_INITIAL_DELAY;
        }
        if (maxRetry <= 0) {
            maxRetry = JMSTaskConstant.DEFAULT_MAX_RETRY;
        }
        if (maxAckRetry <= 0) {
            maxAckRetry = JMSTaskConstant.DEFAULT_MAX_RETRY;
        }
        if (clientAck <= 0) {
            clientAck = JMSTaskConstant.NO_ACKNOWLEDGE;
        } else if (clientAck == JMSTaskConstant.CLIENT_ACKNOWLEDGE) {
            if (replyQueue == null) {
                throw new RuntimeException(
                        "replyQueue must not be null if using client acknoledge mode.");
            }
        }
    }

    /**
     * Stop Spring ThreadPoolTaskScheduler
     */
    @PreDestroy
    private void stop() {
        if (threadPoolTaskScheduler != null) {
            threadPoolTaskScheduler.shutdown();
        }
    }

    /**
     * Update numbers of new tasks to processing, and return the number of tasks
     * for processing.
     * 
     * @return
     */
    private List<JMSTask> loadTasks() {
        jmsTaskDao.updateTaskForProcessing(this.taskType, threads
                * maxTasksPerThread);
        return jmsTaskDao.findTaskForProcessing(this.taskType, threads
                * maxTasksPerThread);
    }

    /**
     * Run the tasks using threads.
     * 
     * @param tasks
     */
    private void excuteTasks(List<JMSTask> tasks) {
        int tSize = tasks.size();
        if (tSize <= 0) {
            return;
        }
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, threads, 60,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(
                        maxTasksPerThread * threads));
        for (final JMSTask task : tasks) {
            final int mRetry = this.maxRetry;
            final int mAckRetry = this.maxAckRetry;
            runFutures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendMessage(task);
                        if (clientAck == JMSTaskConstant.NO_ACKNOWLEDGE) {
                            jmsTaskDao.updateTaskCompeleted(task.getId());
                        } else if (clientAck == JMSTaskConstant.CLIENT_ACKNOWLEDGE) {
                            // if sent maxRetry times, won't wait for
                            // acknowledge, just complete it.
                            jmsTaskDao.updateTaskProcessed(task, mAckRetry);
                        }
                    } catch (Exception e) {
                        // if retried sending maxRetry times, make it fatal, and
                        // no longer retry.
                        task.setLastError(e.getClass().getSimpleName() + ":"
                                + e.getMessage());
                        jmsTaskDao.updateErrorTask(task, mRetry);
                    }
                }
            }));
        }
    }

    /**
     * @return Returns the taskType.
     */
    public int getTaskType() {
        return taskType;
    }

    /**
     * @param taskType
     *            The taskType to set.
     */
    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    /**
     * @return Returns the fixedDelay.
     */
    public long getFixedDelay() {
        return fixedDelay;
    }

    /**
     * @param fixedDelay
     *            The fixedDelay to set.
     */
    public void setFixedDelay(long fixedDelay) {
        this.fixedDelay = fixedDelay;
    }

    /**
     * @return Returns the fixedRate.
     */
    public long getFixedRate() {
        return fixedRate;
    }

    /**
     * @param fixedRate
     *            The fixedRate to set.
     */
    public void setFixedRate(long fixedRate) {
        this.fixedRate = fixedRate;
    }

    /**
     * @return Returns the cron.
     */
    public String getCron() {
        return cron;
    }

    /**
     * @param cron
     *            The cron to set.
     */
    public void setCron(String cron) {
        this.cron = cron;
    }

    /**
     * @return Returns the initialDelay.
     */
    public long getInitialDelay() {
        return initialDelay;
    }

    /**
     * @param initialDelay
     *            The initialDelay to set.
     */
    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }

    /**
     * @return Returns the threads.
     */
    public int getThreads() {
        return threads;
    }

    /**
     * @param threads
     *            The threads to set.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * @return Returns the maxRetry.
     */
    public int getMaxRetry() {
        return maxRetry;
    }

    /**
     * @param maxRetry
     *            The maxRetry to set.
     */
    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    /**
     * @return Returns the maxTasksPerThread.
     */
    public int getMaxTasksPerThread() {
        return maxTasksPerThread;
    }

    /**
     * @param maxTasksPerThread
     *            The maxTasksPerThread to set.
     */
    public void setMaxTasksPerThread(int maxTasksPerThread) {
        this.maxTasksPerThread = maxTasksPerThread;
    }

    /**
     * @return Returns the clientAck.
     */
    public int getClientAck() {
        return clientAck;
    }

    /**
     * @param clientAck
     *            The clientAck to set.
     */
    public void setClientAck(int clientAck) {
        this.clientAck = clientAck;
    }

    /**
     * @return Returns the runFutures.
     */
    public Collection<Future<?>> getRunFutures() {
        return runFutures;
    }

    /**
     * @return Returns the jmsSender.
     */
    public JmsTemplate getJmsSender() {
        return jmsSender;
    }

    /**
     * @param jmsSender
     *            The jmsSender to set.
     */
    public void setJmsSender(JmsTemplate jmsSender) {
        this.jmsSender = jmsSender;
    }

    /**
     * @return Returns the replyQueue.
     */
    public Queue getReplyQueue() {
        return replyQueue;
    }

    /**
     * @param replyQueue
     *            The replyQueue to set.
     */
    public void setReplyQueue(Queue replyQueue) {
        this.replyQueue = replyQueue;
    }

    /**
     * @return Returns the maxAckRetry.
     */
    public int getMaxAckRetry() {
        return maxAckRetry;
    }

    /**
     * @param maxAckRetry
     *            The maxAckRetry to set.
     */
    public void setMaxAckRetry(int maxAckRetry) {
        this.maxAckRetry = maxAckRetry;
    }

}
