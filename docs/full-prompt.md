Create a Kafka Streams application for customer segmentation based on an order event stream.

## Tech Stack
- **Build**: Gradle with Kotlin DSL (`build.gradle.kts`), use `gradle.properties` for version management
- **Language**: Java 17
- **Dependencies**: Kafka Streams 4.1.1, Jackson 2.17.2 for JSON serialization, SLF4J for logging
- **Testing**: JUnit 5, AssertJ, `kafka-streams-test-utils` (TopologyTestDriver)

## Input Model

Orders arrive as JSON on a topic called `orders`:

```java
public class Order {
    public String orderId;
    public String customerId;
    public double amount;
    public long timestamp;
    public String category;
}
```

## Segmentation Logic

Aggregate per `customerId` and compute:
- `total_spent` = SUM(amount)
- `order_count` = COUNT(*)
- `avg_order_value` = AVG(amount)
- `categories_purchased` = COUNT(DISTINCT category)

Classify each customer into a segment:
- **VIP**: total_spent >= 1000 AND order_count >= 10 (both conditions required)
- **Premium**: total_spent >= 500 OR order_count >= 5
- **Regular**: total_spent >= 100 OR order_count >= 2
- **New**: otherwise

Classify purchase behavior:
- **Diverse**: categories_purchased >= 4
- **Multi-Category**: categories_purchased >= 2
- **Single-Category**: otherwise

Emit the enriched customer segment to a `customer-segments` output topic as JSON.

## Architecture Requirements

1. **Topology in a static method**: Create a `CustomerSegmentationTopology` class with a `public static Topology build()` method. This makes the topology testable with TopologyTestDriver without needing a broker. Define topic names and state store name as public constants on this class.

2. **Mutable aggregate accumulator**: Create a `CustomerStats` class that tracks `totalSpent` (double), `orderCount` (int), and `categoriesPurchased` (Set<String>). It should have an `add(Order)` method that mutates and returns `this`. Place the segmentation logic (`computeSegment()`, `computePurchaseBehavior()`) as methods on this class so they are testable without Kafka.

3. **Output model**: Create a `CustomerSegment` class with a `static fromStats(String customerId, CustomerStats stats)` factory method that flattens the Set into a count and precomputes the segment/behavior labels.

4. **Custom JSON Serde**: Build a generic `JsonSerde<T>` wrapping Jackson's ObjectMapper. The `Set<String>` in CustomerStats is handled natively by Jackson since the type info lives on the POJO field. Configure `FAIL_ON_UNKNOWN_PROPERTIES = false` on the deserializer for schema evolution resilience.

5. **Topology flow**: `orders` topic → `selectKey` by customerId → `groupByKey` → `aggregate` into CustomerStats (with a named state store) → `mapValues` to CustomerSegment → `toStream().to()` the output topic.

6. **Auto-create topics**: The app's `main()` should use Kafka `AdminClient` to check for existing topics and create the input/output topics if they don't exist, before starting the KafkaStreams instance.

## Testing Requirements

Two layers of tests:

1. **Pure unit tests** (`CustomerStatsTest`) — no Kafka dependency:
   - Test each segment tier (New, Regular, Premium, VIP)
   - Test that VIP requires BOTH conditions (high spend + low count = Premium, not VIP)
   - Test each purchase behavior tier
   - Test average calculation
   - Test that duplicate categories are not double-counted (Set behavior)
   - Test serde round-trip to verify Set<String> survives Jackson serialization/deserialization

2. **TopologyTestDriver integration tests** (`CustomerSegmentationTopologyTest`):
   - Single order → New segment
   - Progressive orders building up to VIP
   - Premium by spend threshold
   - Regular by order count
   - Multiple customers are independent (different keys)
   - State store contains correct aggregated stats
   - Average order value correctness
   - Categories count in output (with duplicate categories)

Note: TopologyTestDriver emits one output per input (no caching). Tests should account for this by reading through all outputs and asserting on the last one for progressive aggregation tests.

## Verification
Run `./gradlew test` — all tests should pass.

---
