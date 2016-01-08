<html>
<body>
	<h1>${message}</h1>
    Host: <input type='text' id='hostName'/></br>
    Queue: <input type='text' id='queueName' /></br>
    Last Checked at: <input type="text" id='lastCheckedDate'/></br>
    <input type='button' id='getMessages' value='get messages'></br>

    <div id="messageContainer">
    </div>

    <!-- templates -->

    <script id="messages-template" type="text/x-handlebars-template">
        <p>Messages</p>
        <ul>
        {{#messages}}
            <li>{{addedDate}}:: {{message}}</li>
        {{/messages}}
        </ul>
    </script>

    <!-- frameworks -->

    <script src="public/scripts/handlebars.js"></script>

    <!-- scripts -->

    <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>
    <script src="public/scripts/MainApp.js"></script>
    <script src="public/scripts/MessagesClient.js"></script>
    <script src="public/scripts/MessagesClientAdapter.js"></script>
    <script src="public/scripts/TemplateLayer.js"></script>

    <!-- configure this page -->

    <script type="text/javascript">
        $(function() {
            window.MainApp.MessageClientAdapter.bindEvents({
                hostNameId : '#hostName',
                queueNameId : '#queueName',
                lastCheckedDateId : '#lastCheckedDate',
                requestMessagesButtonId : '#getMessages',
                messagesTemplateId : "messages-template",
                messagesContainerId : "#messageContainer"
            });
        });
    </script>

</body>
</html>