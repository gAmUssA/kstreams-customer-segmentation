# Kafka Streams Customer Segmentation

A Kafka Streams application that performs real-time customer segmentation based on order events. It aggregates order data per customer and classifies them into segments with purchase behavior analysis.

## Architecture

```
orders topic ──> Kafka Streams App ──> customer-segments topic
                     │
                     ├─ Group by customerId
                     ├─ Aggregate into CustomerStats (state store)
                     └─ Map to CustomerSegment (Avro)
```

The application reads `Order` events from an input topic, maintains a running aggregate per customer in a state store, and outputs `CustomerSegment` records to an output topic. All messages use Avro serialization with Confluent Schema Registry.

## Segmentation Logic

### Customer Segments

| Segment  | Condition                                  |
|----------|--------------------------------------------|
| VIP      | Total spent >= $1,000 AND order count >= 10 |
| Premium  | Total spent >= $500 OR order count >= 5     |
| Regular  | Total spent >= $100 OR order count >= 2     |
| New      | Everything else                             |

### Purchase Behavior

| Behavior        | Condition               |
|-----------------|-------------------------|
| Diverse         | 4+ distinct categories  |
| Multi-Category  | 2-3 distinct categories |
| Single-Category | 1 category              |

## Avro Schemas

**Order** (input):

| Field      | Type   |
|------------|--------|
| orderId    | string |
| customerId | string |
| amount     | double |
| timestamp  | long   |
| category   | string |

**CustomerSegment** (output):

| Field               | Type   |
|---------------------|--------|
| customerId          | string |
| totalSpent          | double |
| orderCount          | int    |
| avgOrderValue       | double |
| categoriesPurchased | int    |
| customerSegment     | string |
| purchaseBehavior    | string |

## Prerequisites

- Java 17+
- Docker (for integration tests and local Kafka)

## Configuration

Copy the template and edit as needed:

```bash
cp .env.template .env
```

All configuration is loaded from the `.env` file using [dotenv-java](https://github.com/cdimascio/dotenv-java). System environment variables override `.env` values.

| Variable                  | Default                      | Description                              |
|---------------------------|------------------------------|------------------------------------------|
| `MODE`                    | `local`                      | `local` or `cloud` (Confluent Cloud)     |
| `APPLICATION_ID`          | `customer-segmentation-app`  | Kafka Streams application ID             |
| `CC_BOOTSTRAP_SERVER`     | `localhost:9092`             | Kafka bootstrap servers                  |
| `CC_SCHEMA_REGISTRY_URL`  | `http://localhost:8081`      | Schema Registry URL                      |
| `INPUT_TOPIC`             | `orders`                     | Input topic for order events             |
| `OUTPUT_TOPIC`            | `customer-segments`          | Output topic for customer segments       |
| `TOPIC_PARTITIONS`        | `1`                          | Partitions for auto-created topics       |
| `TOPIC_REPLICATION_FACTOR`| `1`                          | Replication factor for auto-created topics|
| `CC_API_KEY`              |                              | Kafka API key (cloud mode only)          |
| `CC_API_SECRET`           |                              | Kafka API secret (cloud mode only)       |
| `CC_SR_API_KEY`           |                              | Schema Registry API key (cloud mode only)|
| `CC_SR_API_SECRET`        |                              | Schema Registry API secret (cloud mode only)|

When `MODE=cloud`, the app automatically configures SASL_SSL authentication for Kafka and basic auth for Schema Registry.

## Running

### Local Mode

1. Start Kafka and Schema Registry locally (e.g., via Docker Compose or Confluent CLI).

2. Create the configuration file:

   ```bash
   cp .env.template .env
   ```

3. Start the streams application:

   ```bash
   ./gradlew run
   ```

4. In a separate terminal, start the sample order producer:

   ```bash
   ./gradlew runKafkaProducer
   ```

   The producer generates realistic order data: 10 customers, 8 product categories, amounts between $5-$500, one order per second.

5. Both the streams app and the producer auto-create topics on startup if they don't exist.

### Confluent Cloud Mode

1. Create the configuration file:

   ```bash
   cp .env.template .env
   ```

2. Edit `.env` with your Confluent Cloud credentials:

   ```bash
   MODE=cloud
   CC_BOOTSTRAP_SERVER=pkc-xxxxx.region.aws.confluent.cloud:9092
   CC_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.aws.confluent.cloud
   CC_API_KEY=your-kafka-api-key
   CC_API_SECRET=your-kafka-api-secret
   CC_SR_API_KEY=your-sr-api-key
   CC_SR_API_SECRET=your-sr-api-secret
   TOPIC_PARTITIONS=6
   TOPIC_REPLICATION_FACTOR=3
   ```

3. Run the apps:

   ```bash
   ./gradlew run
   # In another terminal:
   ./gradlew runKafkaProducer
   ```

### Overriding Configuration

Environment variables override `.env` values, useful for one-off runs:

```bash
MODE=local CC_BOOTSTRAP_SERVER=localhost:9092 ./gradlew run
```

## Testing

Run all tests:

```bash
./gradlew test
```

Docker must be running for integration tests.

### Test Breakdown

| Test Class                                | Tests | Type        | Description                                      |
|-------------------------------------------|-------|-------------|--------------------------------------------------|
| `CustomerStatsTest`                       | 14    | Unit        | Segmentation logic, aggregation, serde round-trips|
| `CustomerSegmentationTopologyTest`        | 9     | Topology    | End-to-end topology with TopologyTestDriver       |
| `CustomerSegmentationIntegrationTest`     | 2     | Integration | Full pipeline with Testcontainers Kafka + SR      |
| `SampleOrderProducerIntegrationTest`      | 2     | Integration | Producer sends valid Avro orders to real Kafka     |

- **Unit tests** run without external dependencies
- **Topology tests** use `TopologyTestDriver` with a mock Schema Registry (`mock://`)
- **Integration tests** use Testcontainers with `ConfluentKafkaContainer` (cp-kafka:8.1.1) and Schema Registry, with Awaitility for async assertions

## Project Structure

```shell
src/main/
  avro/
    order.avsc                          # Order Avro schema
    customer_segment.avsc               # CustomerSegment Avro schema
  java/com/example/segmentation/
    AppConfig.java                      # .env configuration loader
    CustomerSegmentationApp.java        # Main streams application
    CustomerSegmentationTopology.java   # Kafka Streams topology
    SampleOrderProducer.java            # Sample data producer
    model/
      CustomerStats.java                # Aggregation state + segmentation logic
    serde/
      JsonSerde.java                    # JSON serde for state store
      JsonSerializer.java
      JsonDeserializer.java
    util/
      CustomerSegmentMapper.java        # CustomerStats -> Avro CustomerSegment
  resources/
    logback.xml                         # Logging configuration
src/test/
  java/com/example/segmentation/
    CustomerStatsTest.java
    CustomerSegmentationTopologyTest.java
    CustomerSegmentationIntegrationTest.java
    SampleOrderProducerIntegrationTest.java
  resources/
    logback-test.xml                    # Test logging configuration
```

## Tech Stack

- Java 17
- Kafka Streams 4.1.1
- Apache Avro 1.12.1
- Confluent Schema Registry (kafka-streams-avro-serde 8.1.1)
- Logback for logging
- Gradle with Kotlin DSL
- Testcontainers 1.21.4
- JUnit 5, AssertJ, Awaitility

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make your changes
4. Run the tests (`./gradlew test`)
5. Commit and push (`git push origin feature/my-change`)
6. Open a Pull Request

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
