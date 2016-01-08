# simple-amqp-spring-example
A simple Spring application that reads the messages from a AMQP queue

Installing a AMQP (RabbitMQ) server:
------------------------------------

http://www.rabbitmq.com/install-homebrew.html

brew update

brew install rabbitmq

Running a local AMQP server:
----------------------------

cd /usr/local/sbin

./rabbitmq-server

Running the simple consumer example
-----------------------------------

Clone the project

mvn clean install -Pdev

mvn jetty:run -Pdev
