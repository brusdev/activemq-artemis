# AMQP Broker Connection with Receivers

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.

This example demonstrates how you can evenly balance incoming client connections across two brokers
using a third broker with a broker balancer to redirect incoming client connections.