package com.bleum.canton.jms;

import javax.annotation.Resource;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.bleum.canton.common.exception.BusinessException;
import com.bleum.canton.jms.constant.JMSTaskConstant;

/**
 * 
 * Description: Provides default implementations of MessageListener for JMS
 * task.
 * 
 * @author jay.li
 * @created Feb 21, 2014 3:41:41 PM
 * 
 */
public abstract class AbstractTaskMessageListener implements MessageListener {

    private static final Logger LOG = Logger
            .getLogger(AbstractTaskMessageListener.class);

    @Resource
    private ConnectionFactory factory;

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public void onMessage(Message message) {
        LOG.info(this.getClass().getSimpleName() + " receive the message");
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;
            boolean isError = false;
            String errorMsg = "";
            try {
                processMessage(tm);

                // the following exceptions should be all in unrecoverable
                // exception list. If the error can be recover, just leave it to
                // throw. JMS broker (configured as transacted) will detected it
                // and re-deliver the message.
            } catch (JMSException e) {
                // parsing error, don't retry, but catch and swallow
                LOG.error("error happend during processing maessage");
                LOG.error(e.getMessage());
                LOG.error(ExceptionUtils.getStackTrace(e));
                isError = true;
                errorMsg = e.getMessage();
            } catch (BusinessException e) {
                // business error (eg., can't find the item need updating),
                // don't retry, but catch and swallow
                LOG.info("error happend during update inventory for item");
                LOG.error(e.getMessage());
                LOG.error(ExceptionUtils.getStackTrace(e));
                isError = true;
                errorMsg = e.getMessage();
            } finally {
                try {
                    // handle acknowledge
                    if (tm.propertyExists("clientAck")
                            && tm.getIntProperty("clientAck") == JMSTaskConstant.CLIENT_ACKNOWLEDGE) {
                        Connection connection = factory.createConnection();
                        connection.start();
                        Session session = connection.createSession(false,
                                Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer = session.createProducer(tm
                                .getJMSReplyTo());
                        TextMessage om = session.createTextMessage();
                        om.setLongProperty("taskId",
                                tm.getLongProperty("taskId"));
                        om.setBooleanProperty("isError", isError);
                        if (isError) {
                            om.setStringProperty("errorMsg", errorMsg);
                        }
                        producer.send(om);
                    }
                } catch (JMSException e) {
                    LOG.error("error happend during sending reply.");
                    LOG.error(e.getMessage());
                    LOG.error(ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    /**
     * How to process the message, leave it to sub class.
     * 
     * @param tm
     * @throws JMSException
     * @throws BusinessException
     */
    abstract protected void processMessage(TextMessage tm) throws JMSException,
            BusinessException;
}
