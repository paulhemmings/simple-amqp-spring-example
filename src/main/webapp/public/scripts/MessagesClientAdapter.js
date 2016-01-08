(function(app) {

    var config = {};

    function bindEvents(cfg) {

        // initialize the template layer (here?)

        app.TemplateLayer.init();

        // store page configuration

        config.hostNameId = cfg.hostNameId;
        config.queueNameId = cfg.queueNameId;
        config.lastCheckedDateId = cfg.lastCheckedDateId;
        config.requestMessagesButtonId = cfg.requestMessagesButtonId;
        config.messagesTemplateId = cfg.messagesTemplateId;
        config.messagesContainerId = cfg.messagesContainerId;

        // bind page events

        $(document).on('click', config.requestMessagesButtonId, handleMessageSearchRequest);
    }

    function handleMessageSearchRequest(event) {
        var hostName = $(config.hostNameId).val(),
            queueName = $(config.queueNameId).val(),
            lastChecked = $(config.lastCheckedDateId).val();

        app.MessageClient.loadMessages(hostName, queueName, lastChecked, displayNewMessages);
    }

    function displayNewMessages(data) {
        app.TemplateLayer.renderTemplate(
            config.messagesTemplateId,
            config.messagesContainerId,
            data,
            false
        );
    }

    app.MessageClientAdapter = {
        'bindEvents' : bindEvents,
        'requestNewMessages': handleMessageSearchRequest
    }

})(MainApp);