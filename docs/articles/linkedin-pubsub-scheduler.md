# Delayed Message Delivery in Pub/Sub

In the world of pub/sub, sometimes messages need to be delayed. "Send this email in 24 hours or on October 1st." "Retry this payment in 5 minutes." "Run this report every morning at 9am."

Most engineers solve this through cron jobs, database triggers, or custom loops. But there is a better way.

In a Pub/Sub world, ActiveMQ broker has a built-in facility where you can publish a message with an AMQ_SCHEDULED_DELAY header and it will deliver that message via its internal scheduler at a later time. ActiveMQ is a wonderful JMS broker, but it has its limitations. Presently, when everyone is on the Kafka bandwagon, this delayed delivery feature is somewhat missing.

Introducing a standalone pubsub scheduler that offers these features and more.

Send a message to the scheduler topic, tell the scheduler when to deliver it and where, and watch the magic happen. This little component can swap message brokers and data stores and will work with whatever existing infrastructure is already available.

Check it out if you need delayed message delivery.

https://github.com/maxfortun/pubsub-scheduler

Know a better way? Please share!
