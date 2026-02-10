Create a Kafka Streams application for customer segmentation based on an order event stream.

## Tech Stack
- Gradle with Kotlin DSL, Java 17
- Kafka Streams 4.1.1, Jackson for JSON serde
- JUnit 5 + TopologyTestDriver for testing

## Input

JSON orders on an `orders` topic:

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

Aggregate per `customerId`:

```sql
SELECT 
    customerId,
    SUM(amount) as total_spent,
    COUNT(*) as order_count,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT category) as categories_purchased,
    CASE 
        WHEN SUM(amount) >= 1000 AND COUNT(*) >= 10 THEN 'VIP'
        WHEN SUM(amount) >= 500 OR COUNT(*) >= 5 THEN 'Premium'
        WHEN SUM(amount) >= 100 OR COUNT(*) >= 2 THEN 'Regular'
        ELSE 'New'
    END as customer_segment,
    CASE
        WHEN COUNT(DISTINCT category) >= 4 THEN 'Diverse'
        WHEN COUNT(DISTINCT category) >= 2 THEN 'Multi-Category'
        ELSE 'Single-Category'
    END as purchase_behavior
FROM orders
GROUP BY customerId;
```

Emit results to a `customer-segments` topic.

## Requirements

- Build the topology in a static `Topology build()` method so it's testable with TopologyTestDriver without a broker
- Use a mutable aggregate class with a `Set<String>` to track distinct categories
- Place segmentation logic on the aggregate class so it's unit-testable without Kafka
- Auto-create input/output topics on startup using AdminClient if they don't exist
- Build a generic `JsonSerde<T>` using Jackson
- Two test layers: pure unit tests for segmentation logic, and TopologyTestDriver tests for the full pipeline
- Tests must cover all segment tiers, the VIP AND-condition edge case, distinct category deduplication, and serde round-trip
