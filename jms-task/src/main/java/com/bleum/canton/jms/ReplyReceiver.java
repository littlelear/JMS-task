package com.bleum.canton.jms;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.bleum.canton.jms.dao.IJMSTaskDao;

/**
 * 
 * Description: Task reply receiver.
 * 
 * @author jay.li
 * @created Feb 21, 2014 1:35:54 PM
 * 
 */
@Service("replyMessageListener")
public class ReplyReceiver implements MessageListener {

    private static final Logger LOG = Logger.getLogger(ReplyReceiver.class);

    @Resource(name = "jmsTaskDao")
    private IJMSTaskDao jmsTaskDao;

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public void onMessage(Message message) {
        LOG.info("replyReceiver receive the message");
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;
            try {
                long taskId = tm.getLongProperty("taskId");
                boolean isError = tm.getBooleanProperty("isError");
                if (isError) {
                    // if the acknowledge got a error, the error must be
                    // unrecoverable, so set the task to fatal
                    String errorMsg = tm.getStringProperty("errorMsg");
                    jmsTaskDao.updateTaskFatal(taskId, errorMsg);
                } else {
                    LOG.info("Completing task:" + taskId);
                    jmsTaskDao.updateTaskCompeleted(taskId);
                }
            } catch (JMSException e) {
                // parsing error, don't retry, catch and swallow
                LOG.error("Error happend during receiving task reply");
                LOG.error(e.getMessage());
                LOG.error(ExceptionUtils.getStackTrace(e));
            }
        }
    }

}