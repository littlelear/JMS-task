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
package com.bleum.canton.jms.dao;

import java.util.List;

import com.bleum.canton.jms.entity.JMSTask;

/**
 * 
 * Description: JMS Task DAO interface.
 * 
 * @author jay.li
 * @created Feb 19, 2014 3:15:29 PM
 * 
 */
public interface IJMSTaskDao {

    /**
     * 
     * Insert a task record to table
     * 
     * @param task
     */
    void addTask(JMSTask task);

    /**
     * 
     * Update task status by task id
     * 
     * @param status
     */
    void updateTaskStatusByTaskId(long taskId, int statusId);

    /**
     * Update status of the limit number of tasks with specified type for
     * processing
     * 
     * @param type
     * @param limit
     */
    void updateTaskForProcessing(int type, int limit);

    /**
     * Find he limit number of tasks with specified type for processing
     * 
     * @param type
     * @param limit
     * @return
     */
    List<JMSTask> findTaskForProcessing(int type, int limit);

    /**
     * Update tasks to PROCESSED status
     * 
     * @param task
     * @param taskId
     */
    void updateTaskProcessed(JMSTask task, int maxRetry);

    /**
     * Update tasks to ERROR status
     * 
     * @param task
     * @param maxRetry
     */
    void updateErrorTask(JMSTask task, int maxRetry);

    /**
     * Update tasks to COMPELETED status
     * 
     * @param taskId
     */
    void updateTaskCompeleted(long taskId);

    /**
     * Update tasks to FATAL status
     * 
     * @param taskId
     * @param errorMsg
     */
    void updateTaskFatal(long taskId, String errorMsg);

}
