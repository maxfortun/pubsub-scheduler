---
layout: default
title: PubSub Scheduler - Delayed Message Delivery for Kafka & Beyond
---

# PubSub Scheduler

**Delayed and scheduled message delivery for Kafka, ActiveMQ, and any pub/sub system.**

[Get Started](#quick-start){: .btn .btn-primary }
[View on GitHub](https://github.com/maxfortun/pubsub-scheduler){: .btn }

---

## The Problem

You need to send a message in 5 minutes. Or retry a payment at midnight. Or run a job every morning at 9am.

**Kafka doesn't do scheduling.** ActiveMQ has a basic scheduler, but no job ordering, no cancellation, no lifecycle events.

So you build a custom solution. A polling loop. A cron job. A database with timestamps. It works... until it doesn't scale, loses messages, or becomes unmaintainable.

## The Solution

PubSub Scheduler sits between your producers and your message broker. Send a message with `SCHEDULER_*` headers, and it delivers to any destination topic at the right time.

```
SCHEDULER_DESTINATION: orders.process
SCHEDULER_SLEEP: PT5M
```

That's it. The message arrives at `orders.process` in 5 minutes.

---

## Key Features

### Flexible Timing

| Method | Example | Use Case |
|--------|---------|----------|
| **Absolute** | `SCHEDULER_AT: 2026-09-20T15:00:00Z` | Scheduled reports, appointments |
| **Relative** | `SCHEDULER_SLEEP: PT5M` | Retries, cooldowns, debouncing |
| **Recurring** | `SCHEDULER_CRON: 0 9 * * *` | Daily jobs, periodic sync |

### Job Ordering & Deduplication

Control how jobs with the same key behave:

| Policy | Behavior |
|--------|----------|
| **QUEUE** | Chain jobs — each waits for the previous to complete |
| **REPLACE** | Cancel pending jobs, schedule the new one |
| **SKIP** | Drop if a job with this key already exists |

### Production Ready

- **Multi-region HA** with CockroachDB
- **Horizontal scaling** with automatic shard rebalancing
- **Advisory events** for job lifecycle monitoring
- **REST API** for operational control
- **Message transforms** for claim-check patterns

### Broker Agnostic

Works with Kafka, ActiveMQ, RabbitMQ, and [40+ transports](https://camel.apache.org/components/latest/index.html) via Apache Camel.

---

## Quick Start

### Docker (30 seconds)

```bash
# Pull and run
docker pull maxfortun/pubsub-scheduler:latest

# Or use the example compose file
curl -O https://raw.githubusercontent.com/maxfortun/pubsub-scheduler/main/examples/docker-compose.yml
curl -O https://raw.githubusercontent.com/maxfortun/pubsub-scheduler/main/examples/init-postgres.sql
docker-compose up -d
```

### Send a Scheduled Message

```bash
# Schedule a message 5 minutes from now
echo '{"order": 123}' | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic scheduler.in \
  --property parse.headers=true \
  --property 'headers=SCHEDULER_DESTINATION:orders.process,SCHEDULER_SLEEP:PT5M'
```

---

## Use Cases

### Payment Retries (Fintech)

```
SCHEDULER_KEY: payment-456
SCHEDULER_KEY_POLICY: REPLACE
SCHEDULER_SLEEP: PT30S
```

Retry failed payments with exponential backoff. REPLACE ensures only the latest retry is scheduled.

### Abandoned Cart Reminders (E-commerce)

```
SCHEDULER_KEY: cart-user-789
SCHEDULER_KEY_POLICY: REPLACE
SCHEDULER_SLEEP: PT1H
```

Send reminder 1 hour after last cart update. Each cart update resets the timer.

### Daily Reports (SaaS)

```
SCHEDULER_CRON: 0 9 * * *
SCHEDULER_CRON_CONCURRENT: false
```

Run every day at 9am. Never overlap if yesterday's report is still running.

### Rate-Limited API Sync (Integration)

```
SCHEDULER_KEY: api-sync
SCHEDULER_KEY_POLICY: QUEUE
SCHEDULER_SLEEP: PT1M
SCHEDULER_SLEEP_START: PREV
```

Process API calls in order, with 1 minute between each. Perfect for rate-limited APIs.

---

## Pricing

### Community Edition

**Free** for personal use, education, and non-profit organizations.

- All core features
- Community support via GitHub Issues
- Self-hosted only

[Download](https://github.com/maxfortun/pubsub-scheduler){: .btn }

### Commercial License

**Contact for pricing** — required for commercial use.

- All core features
- Priority email support
- License for your organization

[Contact Sales](mailto:scheduler@maxfortun.net){: .btn .btn-primary }

### Enterprise (Coming Soon)

- Multi-region clustering
- Advanced monitoring dashboard
- SSO/SAML integration
- Dedicated support
- SLA guarantees

[Join Waitlist](mailto:scheduler@maxfortun.net?subject=Enterprise%20Waitlist){: .btn }

---

## Architecture

![Architecture]({{ site.baseurl }}/diagrams/images/architecture.png)

Messages flow through IngestProcessor (with optional pre-transform), persist to the database, wait in the job queue, then fire through FireProcessor (with optional post-transform) to the destination.

[View all diagrams](https://github.com/maxfortun/pubsub-scheduler#diagrams)

---

## Compare

| Feature | PubSub Scheduler | ActiveMQ Scheduler | Quartz | Custom Solution |
|---------|------------------|-------------------|--------|-----------------|
| Broker agnostic | Yes | ActiveMQ only | N/A | Varies |
| Job ordering | Yes | No | Limited | Build it |
| Cancel/replace | Yes | No | Yes | Build it |
| Lifecycle events | Yes | No | Limited | Build it |
| Horizontal scaling | Yes | Broker-dependent | Complex | Build it |
| REST API | Yes | JMX only | Limited | Build it |
| Time to production | Minutes | Minutes | Days | Weeks |

---

## Documentation

- [Full README](https://github.com/maxfortun/pubsub-scheduler#readme)
- [Configuration Reference](https://github.com/maxfortun/pubsub-scheduler#configuration-reference)
- [Architecture](https://github.com/maxfortun/pubsub-scheduler/blob/main/ARCHITECTURE.md)
- [API Reference](https://github.com/maxfortun/pubsub-scheduler#instance-api)

---

## License

[PolyForm Noncommercial 1.0.0](https://github.com/maxfortun/pubsub-scheduler/blob/main/LICENSE)

Free for personal use, education, research, and non-profit. Commercial use requires a separate license.

---

<footer>
<p>Built by <a href="https://github.com/maxfortun">Max Fortun</a> | <a href="https://github.com/maxfortun/pubsub-scheduler">GitHub</a> | <a href="mailto:scheduler@maxfortun.net">Contact</a></p>
</footer>
