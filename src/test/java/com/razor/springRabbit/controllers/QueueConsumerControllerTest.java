package com.razor.springRabbit.controllers;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.ui.ModelMap;

@RunWith(MockitoJUnitRunner.class)
public class QueueConsumerControllerTest
{

    @Spy
    QueueConsumerController queueConsumerController;

    @Mock
    ModelMap modelMap;

    @Before
    public void setup() throws Exception {
    }

    @Test
    public void testPrintWelcome() {
        queueConsumerController.printWelcome(modelMap);
    }
}
