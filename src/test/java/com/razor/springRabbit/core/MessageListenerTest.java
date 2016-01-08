package com.razor.springRabbit.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class MessageListenerTest {

    @Spy
    private MessageListener messageListener;

    private List<DatedMessageItem> messageItemList = new ArrayList<DatedMessageItem>();

    @Before
    public void setUp() throws ParseException {
        Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse("2001-01-01");
        Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("2005-01-01");
        Date date3 = new SimpleDateFormat("yyyy-MM-dd").parse("2010-01-01");

        DatedMessageItem message1 = mock(DatedMessageItem.class);
        doReturn(date1).when(message1).getAddedDate();

        DatedMessageItem message2 = mock(DatedMessageItem.class);
        doReturn(date2).when(message2).getAddedDate();

        DatedMessageItem message3 = mock(DatedMessageItem.class);
        doReturn(date3).when(message3).getAddedDate();

        messageItemList.add(message1);
        messageItemList.add(message2);
        messageItemList.add(message3);

        doReturn(messageItemList).when(this.messageListener).getMessageList();
    }

    @Test
    public void testDateCompareLogicSameAsExpected() throws ParseException {
        Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse("2001-01-01");
        Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("2005-01-01");

        Assert.assertEquals(date1.compareTo(date2), -1);
        Assert.assertEquals(date2.compareTo(date1), 1);
        Assert.assertEquals(date1.compareTo(date1), 0);
    }

    @Test
    public void testRecurseMessageListForLastReadMessage() throws ParseException {

        Date lastCheckTime = new SimpleDateFormat("yyyy-MM-dd").parse("2009-01-01");
        int lower = 0;
        int upper = this.messageItemList.size()-1;

        int lastRead = messageListener.recurseMessageListForLastReadMessage(lastCheckTime,
                lower,
                upper);

        Assert.assertEquals(lastRead, 2);
    }
}
