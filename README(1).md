# Kafka Event Monitoring System

A Spring Boot application that publishes events to Apache Kafka,
consumes them asynchronously, tracks event-processing metrics with
Micrometer, and exposes metrics to Prometheus for visualization in
Grafana.

## Features

-   REST endpoint for publishing events to Kafka.
-   Kafka consumer that processes events from the `testy` topic.
-   Custom Micrometer counter: `kafka_events_received_total`.
-   Spring Boot Actuator health, metrics, and Prometheus endpoints.
-   Kafka listener error handling with two retry attempts after the
    initial failure.
-   Grafana dashboard for application and Kafka monitoring.
-   Automated tests for producer behavior, consumer counter behavior,
    and Kafka configuration.

## Technology Stack

-   Java 21
-   Spring Boot and Spring for Apache Kafka
-   Apache Kafka (KRaft mode in the current environment)
-   Micrometer and Spring Boot Actuator
-   Prometheus and Grafana
-   Gradle
-   JUnit 5, Mockito, and Spring MockMvc

## Architecture

``` text
HTTP client
    |
    v
POST /producer/event
    |
    v
Spring Boot Kafka producer
    |
    v
Kafka topic: testy
    |
    v
Spring Kafka listener
    |
    +--> Processes event
    |
    +--> Increments kafka_events_received_total
                  |
                  v
          /actuator/prometheus
                  |
                  v
              Prometheus
                  |
                  v
               Grafana
```

## Requirements

-   JDK 21
-   The Gradle wrapper included in the project
-   A running Kafka broker reachable at `127.0.0.1:9092` from the Spring
    Boot application's environment
-   Prometheus and Grafana to view the monitoring dashboard

> Environment note: in the current development setup, Kafka, Prometheus,
> and Grafana run on an Ubuntu virtual machine. VirtualBox port
> forwarding lets the Windows host access the services. Port-forwarding
> settings may differ on another machine.

## Configuration

The current application configuration includes:

``` properties
spring.kafka.bootstrap-servers=127.0.0.1:9092
spring.kafka.consumer.group-id=metrics-consumer-group
management.endpoints.web.exposure.include=health,metrics,prometheus
```

The producer publishes to topic `testy`. The consumer listens to the
same topic with consumer group `metrics-consumer-group`. The producer
currently uses the key `userEvent` for published records.

### Kafka retry handling

`KafkaConsumerConfig` configures a `DefaultErrorHandler` with a
`FixedBackOff` of 1,000 milliseconds and two retry attempts. A failed
record can therefore have up to three processing attempts in total: the
initial attempt and two retries. After retries are exhausted, the
recovery callback logs the topic, partition, offset, and error.

## Build and Run

Run these commands from the project root in PowerShell.

Build and test:

``` powershell
.\gradlew.bat clean build
```

Run tests only:

``` powershell
.\gradlew.bat :app:test
```

Start the application:

``` powershell
.\gradlew.bat :app:bootRun
```

The application listens on port `8080` with the current configuration.

## API Usage

### Publish an event

**Endpoint:** `POST /producer/event`\
**Content type:** `text/plain`

Example PowerShell request:

``` powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/producer/event" `
  -ContentType "text/plain" `
  -Body "Hello Kafka"
```

A successful request returns HTTP `202 Accepted` with a response similar
to:

``` json
{
  "message": "Event published to Kafka"
}
```

An empty or whitespace-only event returns HTTP `400 Bad Request`. If
publishing fails, the endpoint returns HTTP `503 Service Unavailable`.
The producer waits for the Kafka send operation to complete before
returning success.

## Monitoring

### Spring Boot Actuator

-   Health: `http://localhost:8080/actuator/health`
-   Metrics: `http://localhost:8080/actuator/metrics`
-   Prometheus exposition: `http://localhost:8080/actuator/prometheus`

### Custom event counter

The application exposes:

``` promql
kafka_events_received_total
```

This counter increases when the consumer listener processes an event. It
is not simply an HTTP request counter.

Useful Prometheus queries:

``` promql
kafka_events_received_total
```

``` promql
up{job="spring-boot"}
```

``` promql
sum(rate(http_server_requests_seconds_count{job="spring-boot"}[5m]))
```

The current Prometheus scrape configuration uses job name `spring-boot`
and scrapes the application at `10.0.2.2:8080` from the Ubuntu VM.
Update this target if your network or port-forwarding setup changes.

### Grafana

In the current VirtualBox setup, open Grafana at
`http://localhost:3000`. Configure a Prometheus data source pointing to
`http://localhost:9090`, then open the dashboard named
`Kafka Spring Boot Monitoring`.

Dashboard panels previously configured include JVM heap memory,
application CPU usage, Spring Boot target health, Kafka records
consumed, and HTTP requests per second. A custom counter panel can use
`kafka_events_received_total`.

## End-to-End Verification

1.  Start Kafka and confirm it is reachable at the configured bootstrap
    address.
2.  Start the Spring Boot application.
3.  Send a request to `POST /producer/event`.
4.  Confirm the application console logs the consumed event.
5.  Query `kafka_events_received_total` in Prometheus.
6.  Confirm the Grafana panel updates after the next scrape/refresh
    interval.

## Known Scope and Limitations

-   The producer currently publishes plain-text event bodies.
-   The consumer logs received events and increments a counter; it does
    not persist events to a database.
-   Retry/recovery behavior is configured, but the basic configuration
    tests alone do not prove the full retry sequence under a real
    listener container.
-   VM networking and the Prometheus target are environment-specific.
-   The project uses a fixed topic name (`testy`) and a fixed producer
    bootstrap address in the controller.

## Resume Summary

**Kafka Event Monitoring System \| Java, Spring Boot, Apache Kafka,
Prometheus, Grafana**

Built a Spring Boot REST API that publishes events to Kafka and consumes
them asynchronously. Instrumented consumer processing with Micrometer
and exposed application metrics through Spring Boot Actuator for
Prometheus scraping and Grafana dashboards. Added Kafka listener
retry/recovery configuration and automated tests for API behavior,
counter increments, and configuration setup.
