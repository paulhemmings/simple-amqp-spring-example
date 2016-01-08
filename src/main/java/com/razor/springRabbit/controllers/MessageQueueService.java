package com.razor.springRabbit.controllers;

import com.razor.springRabbit.core.DatedMessageItem;
import com.razor.springRabbit.core.MessageListener;
import com.razor.springRabbit.utils.ControllerResponseBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Controller
public class MessageQueueService {

    public final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

    @Autowired
    private MessageListener messageListener;

    @RequestMapping(value="/messages")
    public @ResponseBody
    String getMessages(final HttpServletRequest request,
                       final HttpServletResponse response) {

        // instantiate response

        ControllerResponseBuilder<List<DatedMessageItem>> model
                = new ControllerResponseBuilder<List<DatedMessageItem>>();
        model.setResultTag("messages");

        // get request parameters.

        String hostName = request.getParameter("hostName");
        String queueName = request.getParameter("queueName");
        Date lastViewed = this.getLastViewedDate(request);

                // start listening. if you change parameters it will clear the cache, and listen
        // on new queue. If already listening, there is no effect.

        this.messageListener.startListening(hostName, queueName);

        // get the list of messages loaded since we last checked

        model.setResult(this.messageListener.getMessageSince(lastViewed));
        model.setSuccess(true);

        // return the list of messages as JSON

        response.setStatus(HttpServletResponse.SC_OK);
        return model.generateJsonResponseString();
    }

    protected Date getLastViewedDate(HttpServletRequest request) {
        try {
            String lastViewedDate = request.getParameter("lastCheckedDate");
            return new SimpleDateFormat(DATE_FORMAT).parse(lastViewedDate);
        } catch(ParseException pas) {
            return new Date(1901, 01, 01);
        }
    }



}
