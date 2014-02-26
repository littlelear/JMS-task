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
package com.bleum.canton.jms.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.bleum.canton.common.util.CantonStringUtils;
import com.bleum.canton.jms.constant.JMSTaskConstant;
import com.bleum.canton.jms.dao.IJMSTaskDao;
import com.bleum.canton.jms.entity.JMSTask;

/**
 * 
 * Description: DAO for JMS task using Spring JDBC template. This class contains
 * business logic and manual transaction management.
 * 
 * @author jay.li
 * @created Feb 19, 2014 10:51:10 AM
 * 
 */
public class JMSTaskDao implements IJMSTaskDao {

    private static final String INSERT_TASK = "insert into jms_b_task (content_id, type, status, created_time, operated_time, message, retry) values (?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_TASK_FOR_PROCESSING = "update jms_b_task set status = "
            + JMSTaskConstant.PROCESSING
            + " where type = ? and status = "
            + JMSTaskConstant.NEW + " order by created_time asc limit ?";
    private static final String UPDATE_TASK_STATUS = "update jms_b_task set status = ?, operated_time = ? where id = ? ";
    private static final String UPDATE_TASK_ERROR = "update jms_b_task set status = "
            + JMSTaskConstant.ERROR
            + ", retry = ?, last_error = ? , operated_time = ? where id = ? ";
    private static final String UPDATE_TASK_FATAL = "update jms_b_task set status = "
            + JMSTaskConstant.FATAL
            + ", last_error = ? , operated_time = ? where id = ? ";
    private static final String QUERY_TASK_FOR_PROCESSING = "select * from jms_b_task where (status = "
            + JMSTaskConstant.PROCESSING
            + " or status = "
            + JMSTaskConstant.ERROR
            + " or status = "
            + JMSTaskConstant.PROCESSED
            + ") and type = ? order by created_time asc limit ?";
    private static final String UPDATE_TASK_COMPLETE = "update jms_b_task set status = "
            + JMSTaskConstant.COMPLETED + " , operated_time = ? where id = ? ";
    private static final String UPDATE_TASK_PROCESSED = "update jms_b_task set status = "
            + JMSTaskConstant.PROCESSED
            + ", retry = ?, operated_time = ? where id = ? ";;

    /**
     * Spring jdbc template
     */
    private JdbcTemplate jdbcTemplate;

    /**
     * Spring platform transaction manager
     */
    private PlatformTransactionManager platformTransactionManager;

    /**
     * Set data source for JMS task table.
     * 
     * @param dataSource
     */
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.platformTransactionManager = new DataSourceTransactionManager(
                dataSource);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.bleum.canton.jms.dao.IJMSTaskDao#addTask(com.bleum.canton.jms.entity
     * .JMSTask)
     */
    @Override
    public void addTask(JMSTask task) {
        this.jdbcTemplate.update(INSERT_TASK, task.getContentId(),
                task.getType(), task.getStatus(), task.getCreatedDate(),
                task.getOperatedDate(), task.getMessage(), task.getRetry());

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#updateTaskForProcessing(int,
     * int)
     */
    @Override
    public void updateTaskForProcessing(int type, int limit) {
        DefaultTransactionDefinition paramTransactionDefinition = new DefaultTransactionDefinition();
        TransactionStatus tstatus = platformTransactionManager
                .getTransaction(paramTransactionDefinition);
        try {
            this.jdbcTemplate.update(UPDATE_TASK_FOR_PROCESSING, type, limit);
            platformTransactionManager.commit(tstatus);
        } catch (Exception e) {
            platformTransactionManager.rollback(tstatus);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#findTaskForProcessing(int, int)
     */
    @Override
    public List<JMSTask> findTaskForProcessing(int type, int limit) {
        return this.jdbcTemplate.query(QUERY_TASK_FOR_PROCESSING, new Object[] {
                type, limit }, new RowMapper<JMSTask>() {
            public JMSTask mapRow(ResultSet rs, int rowNum) throws SQLException {
                JMSTask task = new JMSTask();
                task.setId(rs.getLong("id"));
                task.setContentId(rs.getString("content_id"));
                task.setType(rs.getInt("type"));
                task.setStatus(rs.getInt("status"));
                task.setCreatedDate(rs.getDate("created_time"));
                task.setOperatedDate(rs.getDate("operated_time"));
                if (rs.getClob("message") != null) {
                    task.setMessage(CantonStringUtils.convertToString(rs
                            .getClob("message")));
                }
                task.setRetry(rs.getInt("retry"));
                task.setLastError(rs.getString("last_error"));
                return task;
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#updateTaskStatusByTaskId(long,
     * int)
     */
    @Override
    public void updateTaskStatusByTaskId(long taskId, int statusId) {
        DefaultTransactionDefinition paramTransactionDefinition = new DefaultTransactionDefinition();
        TransactionStatus tstatus = platformTransactionManager
                .getTransaction(paramTransactionDefinition);
        try {
            this.jdbcTemplate.update(UPDATE_TASK_STATUS, statusId, new Date(),
                    taskId);
            platformTransactionManager.commit(tstatus);
        } catch (Exception e) {
            platformTransactionManager.rollback(tstatus);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#updateProcessedTask(long)
     */
    @Override
    public void updateTaskProcessed(JMSTask task, int maxRetry) {
        DefaultTransactionDefinition paramTransactionDefinition = new DefaultTransactionDefinition();
        TransactionStatus tstatus = platformTransactionManager
                .getTransaction(paramTransactionDefinition);
        try {
            if (task.getRetry() >= maxRetry) {
                this.jdbcTemplate.update(UPDATE_TASK_COMPLETE, new Date(),
                        task.getId());
            } else {
                this.jdbcTemplate.update(UPDATE_TASK_PROCESSED,
                        task.getRetry() + 1, new Date(), task.getId());
            }
            platformTransactionManager.commit(tstatus);
        } catch (Exception e) {
            platformTransactionManager.rollback(tstatus);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.bleum.canton.jms.dao.IJMSTaskDao#updateErrorTask(com.bleum.canton
     * .jms.entity.JMSTask, int)
     */
    @Override
    public void updateErrorTask(JMSTask task, int maxRetry) {
        DefaultTransactionDefinition paramTransactionDefinition = new DefaultTransactionDefinition();
        TransactionStatus tstatus = platformTransactionManager
                .getTransaction(paramTransactionDefinition);
        try {
            if (task.getRetry() >= maxRetry) {
                this.jdbcTemplate.update(UPDATE_TASK_FATAL,
                        task.getLastError(), new Date(), task.getId());
            } else {
                this.jdbcTemplate.update(UPDATE_TASK_ERROR,
                        task.getRetry() + 1, task.getLastError(), new Date(),
                        task.getId());
            }
            platformTransactionManager.commit(tstatus);
        } catch (Exception e) {
            platformTransactionManager.rollback(tstatus);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#updateTaskCompeleted(long)
     */
    @Override
    public void updateTaskCompeleted(long taskId) {
        updateTaskStatusByTaskId(taskId, JMSTaskConstant.COMPLETED);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.bleum.canton.jms.dao.IJMSTaskDao#updateTaskFatal(long,
     * java.lang.String)
     */
    @Override
    public void updateTaskFatal(long taskId, String errorMsg) {
        DefaultTransactionDefinition paramTransactionDefinition = new DefaultTransactionDefinition();
        TransactionStatus tstatus = platformTransactionManager
                .getTransaction(paramTransactionDefinition);
        try {
            this.jdbcTemplate.update(UPDATE_TASK_FATAL, errorMsg, new Date(),
                    taskId);
            platformTransactionManager.commit(tstatus);
        } catch (Exception e) {
            platformTransactionManager.rollback(tstatus);
        }

    }

}
