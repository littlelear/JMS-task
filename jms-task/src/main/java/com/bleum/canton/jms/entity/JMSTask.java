package com.bleum.canton.jms.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * 
 * Description: JMS Task entity.
 * 
 * @author jay.li
 * @created Feb 19, 2014 10:45:58 AM
 * 
 */
public class JMSTask implements Serializable {

    private static final long serialVersionUID = -1864027040168704495L;

    private long id;

    /**
     * id for JMS message load (eg., order id, customer id).
     */
    private String contentId;

    /**
     * task type
     */
    private int type;

    /**
     * task status
     */
    private int status;

    /**
     * created time
     */
    private Date createdTime;

    /**
     * operated time
     */
    private Date operatedTime;

    /**
     * XML message to send
     */
    private String message;

    /**
     * retried times, stand for both error retry and acknowledge retry
     */
    private int retry;

    /**
     * last error recorded
     */
    private String lastError;

    /**
     * @return Returns the id.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id
     *            The id to set.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Returns the contentId.
     */
    public String getContentId() {
        return contentId;
    }

    /**
     * @param contentId
     *            The contentId to set.
     */
    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    /**
     * @return Returns the type.
     */
    public int getType() {
        return type;
    }

    /**
     * @param type
     *            The type to set.
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * @return Returns the status.
     */
    public int getStatus() {
        return status;
    }

    /**
     * @param status
     *            The status to set.
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return Returns the operatedDate.
     */
    public Date getOperatedDate() {
        return operatedTime == null ? null : (Date) operatedTime.clone();
    }

    /**
     * @param operatedDate
     *            The operatedDate to set.
     */
    public void setOperatedDate(Date operatedDate) {
        this.operatedTime = (operatedDate == null ? null : (Date) operatedDate
                .clone());
    }

    /**
     * @return the createdDate
     */
    public Date getCreatedDate() {
        return createdTime == null ? null : (Date) createdTime.clone();
    }

    /**
     * @param createdDate
     *            the createdDate to set
     */
    public void setCreatedDate(Date createdDate) {
        this.createdTime = (createdDate == null ? null : (Date) createdDate
                .clone());
    }

    /**
     * @return Returns the message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message
     *            The message to set.
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return Returns the retry.
     */
    public int getRetry() {
        return retry;
    }

    /**
     * @param retry
     *            The retry to set.
     */
    public void setRetry(int retry) {
        this.retry = retry;
    }

    /**
     * @return Returns the lastError.
     */
    public String getLastError() {
        return lastError;
    }

    /**
     * @param lastError
     *            The lastError to set.
     */
    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

}
