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
package com.bleum.canton.jms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import com.bleum.canton.jms.dao.IJMSTaskDao;

/**
 * Description: Unit test for ReplyReceiver.
 * 
 * @author jay.li
 * @created Aug 2, 2013 3:04:35 PM
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.management.*", "javax.xml.parsers.*",
        "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*",
        "org.slf4j.*" })
public class ReplyReceiverTest {

    @Mock
    private IJMSTaskDao jmsTaskDao;

    @InjectMocks
    private ReplyReceiver replyReceiver = new ReplyReceiver();

    @Test
    public void onMessage1() throws JMSException {
        Message message = mock(TextMessage.class);
        long taskId = 1;
        boolean isError = true;
        String errorMsg = "my error";
        when(message.getLongProperty("taskId")).thenReturn(taskId);
        when(message.getBooleanProperty("isError")).thenReturn(isError);
        when(message.getStringProperty("errorMsg")).thenReturn(errorMsg);

        replyReceiver.onMessage(message);

        verify(jmsTaskDao).updateTaskFatal(taskId, errorMsg);
    }

    @Test
    public void onMessage2() throws JMSException {
        Message message = mock(TextMessage.class);
        long taskId = 1;
        boolean isError = false;
        when(message.getLongProperty("taskId")).thenReturn(taskId);
        when(message.getBooleanProperty("isError")).thenReturn(isError);

        replyReceiver.onMessage(message);

        verify(jmsTaskDao).updateTaskCompeleted(taskId);
    }

    @Test
    public void onMessage3() throws JMSException {
        Message message = mock(TextMessage.class);

        when(message.getLongProperty("taskId")).thenThrow(
                new JMSException("test"));

        replyReceiver.onMessage(message);

        // swallow JMSException
    }

}
