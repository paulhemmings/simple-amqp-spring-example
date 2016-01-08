(function(app) {

    function messagesFailedToLoad(callback) {

    }

    function loadMessages(hostName, queueName, lastCheckedDate, callback) {
        $.ajax({
            url: '/messages',
            data: {
                'hostName' : hostName,
                'queueName' : queueName,
                'lastCheckedDate' : lastCheckedDate
            },
            success: callback,
            dataType: 'JSON'
        });
    };


    app.MessageClient = {
        loadMessages: loadMessages
    };

})(MainApp);