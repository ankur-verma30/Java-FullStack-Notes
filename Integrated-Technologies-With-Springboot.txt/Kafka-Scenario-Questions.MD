# Kafka Interview Questions - Scenario Based

## 1. Basic Concepts

### Q1: Explain the architecture of Kafka. How does data flow from producer to consumer?

**Answer:**
Kafka architecture consists of:

1. **Producers**: Applications that publish data to topics
2. **Brokers**: Kafka servers that store data and serve requests
3. **Topics**: Categories where records are published
4. **Partitions**: Topics are split into ordered, immutable sequences
5. **Consumers**: Applications that subscribe to topics and process data
6. **ZooKeeper/KRaft**: Cluster coordination and metadata management

**Data Flow:**
```
Producer → Topic (Partition 0, 1, 2...) → Consumer Group → Consumers
```

1. Producer creates a message
2. Message is serialized (key and value)
3. Partitioner determines which partition (based on key hash or round-robin)
4. Message is sent to the leader broker of that partition
5. Leader appends message to its log
6. Followers replicate the message
7. Once replicated to min.insync.replicas, producer gets acknowledgment
8. Consumers in consumer groups pull messages from partitions
9. Each partition is consumed by only one consumer in the group
10. Consumers commit offsets after processing


### Q2: What is the difference between a topic and a partition?

**Answer:**
- **Topic**: A logical category or feed name. Think of it as a table in a database.
- **Partition**: A physical division of a topic. Each partition is an ordered, immutable sequence of records.

**Key differences:**
- Topics can have multiple partitions (configurable)
- Each partition can be on a different broker (distribution)
- Ordering is guaranteed within a partition, not across partitions
- Partitions enable parallel processing
- More partitions = more parallelism but more overhead

**Example:**
```
Topic: "orders"
  ├── Partition 0: [msg1, msg2, msg5, ...]
  ├── Partition 1: [msg3, msg6, msg8, ...]
  └── Partition 2: [msg4, msg7, msg9, ...]
```


### Q3: How does Kafka achieve high throughput?

**Answer:**
1. **Sequential I/O**: Writes are sequential, not random (very fast on disk)
2. **Zero-copy**: Data transferred from disk to network without copying to application memory
3. **Batch Processing**: Messages are batched for efficiency
4. **Compression**: Reduces network bandwidth (snappy, gzip, lz4, zstd)
5. **Partitioning**: Parallel processing across multiple partitions
6. **Page Cache**: Uses OS page cache effectively
7. **Asynchronous Processing**: Non-blocking I/O operations


## 2. Producer Scenarios

### Q4: How would you ensure messages are not lost when sending to Kafka?

**Answer:**
Configure producer for maximum durability:

```java
props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Retry indefinitely
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Prevent duplicates
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 minutes

// Topic configuration
props.put("min.insync.replicas", "2"); // At least 2 replicas must acknowledge
props.put("replication.factor", "3"); // 3 copies of data
```

**Use synchronous send for critical data:**
```java
SendResult<String, Object> result = kafkaTemplate.send(topic, key, value).get();
```


### Q5: A producer is sending 1 million messages per second. Some messages are being lost. How do you diagnose and fix this?

**Answer:**

**Diagnosis steps:**
1. Check producer logs for errors
2. Monitor producer metrics: record-error-rate, buffer-exhausted-rate
3. Check if buffer memory is exhausted
4. Verify acknowledgment settings (acks=all?)
5. Check broker logs for errors
6. Monitor network issues

**Potential causes and fixes:**

**1. Buffer Overflow:**
```java
// Increase buffer memory
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64 MB

// Increase batch size for better throughput
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32 KB

// Adjust linger time
props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
```

**2. Timeout Issues:**
```java
// Increase timeout
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 180000);
```

**3. Not waiting for acknowledgment:**
```java
// Use callbacks to track failures
kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Failed to send: {}", ex.getMessage());
        // Implement retry logic or store in DB
    }
});
```

**4. Add monitoring:**
```java
// Implement custom callback to track success/failure
CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, value);
future.whenComplete((result, ex) -> {
    if (ex != null) {
        metrics.incrementCounter("kafka.producer.failures");
    } else {
        metrics.incrementCounter("kafka.producer.success");
    }
});
```


### Q6: How do you ensure message ordering in Kafka?

**Answer:**

**Key point:** Ordering is guaranteed only within a partition.

**Solution 1: Use message key**
```java
// Messages with same key go to same partition
UserEvent event = new UserEvent();
event.setUserId("user123");

// Use userId as key - all messages for user123 go to same partition
kafkaTemplate.send(topic, event.getUserId(), event);
```

**Solution 2: Single partition (not recommended for scale)**
```java
// Create topic with 1 partition
NewTopic topic = TopicBuilder.name("ordered-topic")
    .partitions(1) // Only 1 partition
    .build();
```

**Solution 3: Custom partitioner**
```java
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes,
                        Cluster cluster) {
        // Custom logic to determine partition
        String userId = (String) key;
        int partition = Math.abs(userId.hashCode()) % numPartitions;
        return partition;
    }
}
```

**Solution 4: Enable idempotence and limit in-flight requests**
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Strict ordering
```


## 3. Consumer Scenarios

### Q7: Consumer is lagging behind. How do you identify and fix consumer lag?

**Answer:**

**Identify lag:**
```bash
# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-consumer-group

# Output shows:
# TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
```

**Causes and solutions:**

**1. Slow processing:**
```java
// Solution: Increase concurrency
@KafkaListener(
    topics = "my-topic",
    groupId = "my-group",
    concurrency = "10" // 10 parallel consumers
)
public void consume(Message message) {
    // Process message
}
```

**2. Insufficient consumers:**
```java
// Add more consumer instances
// Rule: # of consumers <= # of partitions
// If topic has 10 partitions, you can have up to 10 consumers in group
```

**3. Large messages:**
```java
// Increase max.poll.records
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); // Process fewer records per poll

// Increase max.poll.interval.ms
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000); // 10 minutes
```

**4. Network/throughput issues:**
```java
// Increase fetch size
props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); // 1 MB
props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
```

**5. Batch processing:**
```java
@KafkaListener(
    topics = "my-topic",
    groupId = "my-group",
    containerFactory = "batchListenerContainerFactory"
)
public void consumeBatch(List<Message> messages) {
    // Process batch more efficiently
    bulkProcess(messages);
}
```


### Q8: How do you handle consumer rebalancing?

**Answer:**

**What is rebalancing?**
When a consumer joins/leaves a group, partitions are redistributed among active consumers.

**Problems with rebalancing:**
- All consumers stop consuming during rebalance
- Duplicates if offsets not committed
- Increased latency

**Solutions:**

**1. Tune session timeout:**
```java
// Increase session timeout to avoid unnecessary rebalances
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000); // 45 seconds
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15000); // 15 seconds
```

**2. Increase max.poll.interval.ms:**
```java
// If processing takes long, increase this
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000); // 10 minutes
```

**3. Static membership (Kafka 2.3+):**
```java
// Assign static group.instance.id to prevent rebalance on restart
props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "consumer-1");
```

**4. Implement rebalance listeners:**
```java
@KafkaListener(topics = "my-topic", groupId = "my-group")
public void consume(ConsumerRecord<String, String> record,
                   Acknowledgment ack,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    // Process record
    ack.acknowledge();
}

// Add rebalance listener
consumer.subscribe(Arrays.asList("my-topic"), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Save state before rebalance
        log.info("Partitions revoked: {}", partitions);
        // Commit offsets
    }
    
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Initialize state after rebalance
        log.info("Partitions assigned: {}", partitions);
    }
});
```


### Q9: How do you implement exactly-once processing in Kafka?

**Answer:**

**Exactly-once requires:**
1. Idempotent producer
2. Transactions
3. Transactional consumer (read-process-write)

**Implementation:**

**1. Producer configuration:**
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

**2. Consumer configuration:**
```java
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

**3. Transactional processing:**
```java
@Service
public class ExactlyOnceService {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Transactional
    public void processMessage(String inputTopic, String outputTopic, 
                              ConsumerRecord<String, String> record) {
        try {
            // Process the record
            String processedValue = process(record.value());
            
            // Send to output topic in transaction
            kafkaTemplate.executeInTransaction(operations -> {
                operations.send(outputTopic, record.key(), processedValue);
                return true;
            });
            
            // Commit offset in transaction
            // This ensures exactly-once semantics
            
        } catch (Exception e) {
            // Transaction will be rolled back
            log.error("Processing failed: {}", e.getMessage());
            throw e;
        }
    }
}
```

**4. Spring Kafka way:**
```java
@Configuration
public class KafkaConfig {
    
    @Bean
    public ChainedKafkaTransactionManager<String, Object> chainedTransactionManager(
            KafkaTransactionManager<String, Object> kafkaTransactionManager) {
        return new ChainedKafkaTransactionManager<>(kafkaTransactionManager);
    }
}

@Service
public class TransactionalService {
    
    @KafkaListener(topics = "input-topic", groupId = "eos-group")
    @Transactional("chainedTransactionManager")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // Process
        String result = process(record.value());
        
        // Send to output
        kafkaTemplate.send("output-topic", record.key(), result);
        
        // Acknowledge
        ack.acknowledge();
    }
}
```


## 4. Production Issues

### Q10: Your Kafka cluster is running out of disk space. What do you do?

**Answer:**

**Immediate actions:**
1. Check retention settings
2. Increase retention cleanup frequency
3. Enable compression
4. Add more disk space

**Long-term solutions:**

**1. Adjust retention:**
```bash
# Per topic
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=86400000  # 1 day

# Or retention by size
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.bytes=1073741824  # 1 GB per partition
```

**2. Enable compression:**
```yaml
spring:
  kafka:
    producer:
      compression-type: snappy  # or lz4, gzip, zstd
```

**3. Log compaction (for compacted topics):**
```bash
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config cleanup.policy=compact
```

**4. Archive old data:**
- Use Kafka Connect to archive to S3/HDFS
- Use Kafka Tiered Storage (Kafka 3.6+)

**5. Delete old topics:**
```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic old-topic
```


### Q11: One of your brokers crashed. How does Kafka handle this?

**Answer:**

**Kafka's fault tolerance mechanisms:**

**1. Replication:**
- Each partition has replicas on different brokers
- One replica is the leader, others are followers
- If leader crashes, a follower becomes the new leader

**2. In-Sync Replicas (ISR):**
- Replicas that are caught up with the leader
- Only ISR members can become leader
- Controlled by `replica.lag.time.max.ms`

**3. Leader Election:**
- Controller broker manages leader election
- Uses ZooKeeper/KRaft for coordination
- New leader is elected from ISR

**4. Recovery process:**
```
Broker 1 (Leader for P0) crashes
↓
Controller detects failure
↓
Elects new leader from ISR (Broker 2)
↓
Producer/consumers switch to new leader
↓
When Broker 1 recovers, it becomes follower
↓
Syncs data from new leader
↓
Rejoins ISR when caught up
```

**Configuration for high availability:**
```yaml
# Topic configuration
replication.factor: 3
min.insync.replicas: 2  # At least 2 replicas must acknowledge

# Producer configuration
acks: all  # Wait for all ISR replicas

# Broker configuration
unclean.leader.election.enable: false  # Don't allow out-of-sync replica to become leader
```


### Q12: Your application needs to process messages from Kafka even when the database is down. How do you handle this?

**Answer:**

**Solution: Implement retry with backoff and dead letter queue**

```java
@Service
@Slf4j
public class ResilientConsumer {
    
    @Autowired
    private DatabaseService databaseService;
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    private static final int MAX_RETRIES = 3;
    private static final String DLT_TOPIC = "dead-letter-topic";
    
    @KafkaListener(
        topics = "my-topic",
        groupId = "resilient-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        
        boolean success = processWithRetry(record, 0);
        
        if (success) {
            ack.acknowledge();
        } else {
            // Send to DLT after all retries exhausted
            sendToDeadLetterTopic(record);
            ack.acknowledge(); // Acknowledge to move forward
        }
    }
    
    private boolean processWithRetry(ConsumerRecord<String, String> record, int attempt) {
        try {
            // Try to save to database
            databaseService.save(record.value());
            log.info("Successfully processed message");
            return true;
            
        } catch (DatabaseException e) {
            log.error("Database error (attempt {}): {}", attempt + 1, e.getMessage());
            
            if (attempt < MAX_RETRIES) {
                // Exponential backoff
                long sleepTime = (long) Math.pow(2, attempt) * 1000; // 1s, 2s, 4s
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                
                // Retry
                return processWithRetry(record, attempt + 1);
            } else {
                log.error("Max retries exhausted for message: {}", record.key());
                return false;
            }
        }
    }
    
    private void sendToDeadLetterTopic(ConsumerRecord<String, String> record) {
        try {
            Message<String> dltMessage = MessageBuilder
                .withPayload(record.value())
                .setHeader(KafkaHeaders.TOPIC, DLT_TOPIC)
                .setHeader("original-topic", record.topic())
                .setHeader("original-partition", record.partition())
                .setHeader("original-offset", record.offset())
                .setHeader("failure-reason", "Database unavailable after retries")
                .setHeader("timestamp", System.currentTimeMillis())
                .build();
            
            kafkaTemplate.send(dltMessage);
            log.info("Message sent to DLT");
            
        } catch (Exception e) {
            log.error("Failed to send to DLT: {}", e.getMessage());
        }
    }
}
```

**Alternative: Use Spring Retry:**

```java
@Configuration
@EnableRetry
public class RetryConfig {
    
    @Bean
    public DefaultErrorHandler errorHandler() {
        // Exponential backoff: 1s, 2s, 4s
        BackOff backOff = new ExponentialBackOff(1000, 2.0);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate()),
            backOff
        );
        
        // Don't retry for these exceptions
        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class
        );
        
        return errorHandler;
    }
}

@Service
public class RetryableConsumer {
    
    @Retryable(
        value = {DatabaseException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(String message, Acknowledgment ack) {
        // Process message
        databaseService.save(message);
        ack.acknowledge();
    }
    
    @Recover
    public void recover(DatabaseException e, String message) {
        log.error("Recovery: sending to DLT after all retries");
        // Send to DLT
    }
}
```


## 5. Design Scenarios

### Q13: Design a real-time order processing system using Kafka

**Answer:**

**Architecture:**
```
Order Service → Kafka → [Multiple Consumers]
                         ├─> Inventory Service
                         ├─> Payment Service
                         ├─> Notification Service
                         └─> Analytics Service
```

**Topics:**
1. `order-created` - New orders
2. `order-confirmed` - Payment confirmed
3. `order-shipped` - Shipped orders
4. `order-delivered` - Delivered orders
5. `order-cancelled` - Cancelled orders

**Implementation:**

```java
// Order Event Model
@Data
public class OrderEvent {
    private String orderId;
    private String userId;
    private String status;
    private BigDecimal amount;
    private List<OrderItem> items;
    private LocalDateTime timestamp;
}

// Order Service (Producer)
@Service
public class OrderService {
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    public void createOrder(OrderRequest request) {
        // Create order
        Order order = orderRepository.save(request);
        
        // Publish event
        OrderEvent event = OrderEvent.builder()
            .orderId(order.getId())
            .userId(order.getUserId())
            .status("CREATED")
            .amount(order.getAmount())
            .items(order.getItems())
            .timestamp(LocalDateTime.now())
            .build();
        
        kafkaTemplate.send("order-created", order.getId(), event);
    }
}

// Inventory Service (Consumer)
@Service
public class InventoryConsumer {
    
    @KafkaListener(topics = "order-created", groupId = "inventory-group")
    public void handleOrderCreated(OrderEvent event, Acknowledgment ack) {
        try {
            // Reserve inventory
            for (OrderItem item : event.getItems()) {
                inventoryService.reserve(item.getProductId(), item.getQuantity());
            }
            
            // Publish inventory-reserved event
            kafkaTemplate.send("inventory-reserved", event.getOrderId(), event);
            
            ack.acknowledge();
            
        } catch (InsufficientInventoryException e) {
            // Publish inventory-failed event
            kafkaTemplate.send("order-cancelled", event.getOrderId(), event);
            ack.acknowledge();
        }
    }
}

// Payment Service (Consumer)
@Service
public class PaymentConsumer {
    
    @KafkaListener(topics = "inventory-reserved", groupId = "payment-group")
    @Transactional
    public void handleInventoryReserved(OrderEvent event, Acknowledgment ack) {
        try {
            // Process payment
            PaymentResult result = paymentService.charge(
                event.getUserId(),
                event.getAmount()
            );
            
            if (result.isSuccess()) {
                // Update order status
                event.setStatus("CONFIRMED");
                kafkaTemplate.send("order-confirmed", event.getOrderId(), event);
            } else {
                // Release inventory and cancel order
                kafkaTemplate.send("inventory-release", event.getOrderId(), event);
                kafkaTemplate.send("order-cancelled", event.getOrderId(), event);
            }
            
            ack.acknowledge();
            
        } catch (PaymentException e) {
            log.error("Payment failed: {}", e.getMessage());
            // Don't ack - will be retried
        }
    }
}

// Notification Service (Consumer)
@Service
public class NotificationConsumer {
    
    @KafkaListener(topics = {
        "order-created",
        "order-confirmed",
        "order-shipped",
        "order-delivered",
        "order-cancelled"
    }, groupId = "notification-group")
    public void handleOrderEvent(OrderEvent event, 
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 Acknowledgment ack) {
        
        // Send appropriate notification based on event type
        switch (topic) {
            case "order-created":
                sendEmail(event.getUserId(), "Order Received", event);
                break;
            case "order-confirmed":
                sendEmail(event.getUserId(), "Payment Confirmed", event);
                break;
            case "order-shipped":
                sendEmail(event.getUserId(), "Order Shipped", event);
                break;
            case "order-delivered":
                sendEmail(event.getUserId(), "Order Delivered", event);
                break;
            case "order-cancelled":
                sendEmail(event.getUserId(), "Order Cancelled", event);
                break;
        }
        
        ack.acknowledge();
    }
}
```

**Key considerations:**
1. Use saga pattern for distributed transactions
2. Implement compensation logic for failures
3. Use dead letter queues for failed messages
4. Monitor consumer lag
5. Implement idempotency keys to prevent duplicate processing


### Q14: How would you migrate from a monolithic architecture to Kafka-based microservices?

**Answer:**

**Phased migration approach:**

**Phase 1: Dual-write pattern**
```java
@Service
public class OrderService {
    
    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    @Transactional
    public void createOrder(OrderRequest request) {
        // 1. Save to database (existing logic)
        Order order = orderRepository.save(request);
        
        // 2. Also publish to Kafka (new logic)
        OrderEvent event = mapToEvent(order);
        kafkaTemplate.send("order-events", order.getId(), event);
        
        // Both systems receive data
    }
}
```

**Phase 2: Change Data Capture (CDC)**
```yaml
# Use Debezium to capture database changes
connector.class: io.debezium.connector.mysql.MySqlConnector
database.hostname: localhost
database.port: 3306
database.user: debezium
database.password: dbz
database.server.id: 184054
database.server.name: mydb
table.include.list: mydb.orders
```

**Phase 3: Event-driven microservices**
```java
// New microservice reads from Kafka
@Service
public class NewOrderService {
    
    @KafkaListener(topics = "order-events", groupId = "new-service-group")
    public void handleOrderEvent(OrderEvent event, Acknowledgment ack) {
        // Process in new system
        processOrder(event);
        ack.acknowledge();
    }
}
```

**Phase 4: Cutover**
- Verify both systems produce same results
- Switch reads to new system
- Deprecate old system
- Remove dual-write logic


## 6. Performance & Optimization

### Q15: How do you optimize Kafka for high throughput?

**Answer:**

**Producer optimization:**
```java
// Batch messages
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32 KB
props.put(ProducerConfig.LINGER_MS_CONFIG, 50); // Wait 50ms to batch

// Compression
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

// Increase buffer
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64 MB

// Asynchronous send
kafkaTemplate.send(topic, key, value); // Don't block
```

**Consumer optimization:**
```java
// Batch consuming
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

// Increase fetch size
props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); // 1 MB
props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

// Parallel processing
@KafkaListener(
    topics = "my-topic",
    concurrency = "10"
)
```

**Broker optimization:**
```properties
# Increase threads
num.network.threads=8
num.io.threads=16

# Increase replica fetcher threads
num.replica.fetchers=4

# Log segment size
log.segment.bytes=1073741824  # 1 GB

# Socket buffer sizes
socket.send.buffer.bytes=1048576  # 1 MB
socket.receive.buffer.bytes=1048576  # 1 MB
```

**Infrastructure:**
- Use SSD for better I/O
- Separate disk for logs
- Adequate RAM for page cache
- High bandwidth network


### Q16: You notice increased latency in message delivery. How do you troubleshoot?

**Answer:**

**Step-by-step troubleshooting:**

**1. Check producer metrics:**
```bash
# Using JMX
kafka-producer-perf-test.sh --topic test-topic \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092
```

**2. Check broker metrics:**
- Request latency
- Partition count
- Under-replicated partitions
- Network utilization

**3. Check consumer lag:**
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-group
```

**4. Common causes and fixes:**

**Network latency:**
```java
// Increase timeout
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
```

**Too many small messages:**
```java
// Enable batching
props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
```

**Slow consumers:**
```java
// Increase concurrency
@KafkaListener(concurrency = "15")
```

**Under-replicated partitions:**
```bash
# Increase replica fetcher threads
num.replica.fetchers=8
```

**Large messages:**
```java
// Increase max request size
props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760); // 10 MB
```


## 7. Security & Monitoring

### Q17: How do you secure Kafka?

**Answer:**

**1. Authentication (SASL):**
```properties
# Server configuration
listeners=SASL_SSL://localhost:9093
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN

# Client configuration
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
  required username="admin" password="admin-secret";
```

**2. Authorization (ACLs):**
```bash
# Grant permissions
kafka-acls.sh --bootstrap-server localhost:9093 \
  --add \
  --allow-principal User:Alice \
  --operation Read \
  --operation Write \
  --topic my-topic
```

**3. Encryption (SSL/TLS):**
```properties
# Enable SSL
ssl.keystore.location=/var/private/ssl/kafka.server.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
ssl.truststore.location=/var/private/ssl/kafka.server.truststore.jks
ssl.truststore.password=truststore-password
```

**4. Spring Boot configuration:**
```yaml
spring:
  kafka:
    security:
      protocol: SASL_SSL
    properties:
      sasl:
        mechanism: PLAIN
        jaas:
          config: org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="secret";
      ssl:
        truststore:
          location: classpath:kafka.client.truststore.jks
          password: changeit
```


### Q18: How do you monitor Kafka in production?

**Answer:**

**Key metrics to monitor:**

**Broker metrics:**
- Under-replicated partitions (should be 0)
- Offline partitions (should be 0)
- Active controller count (should be 1)
- Request rate and latency
- Bytes in/out per second
- CPU and memory usage
- Disk usage

**Producer metrics:**
- Record send rate
- Record error rate
- Request latency
- Buffer available bytes

**Consumer metrics:**
- Consumer lag
- Records consumed rate
- Commit latency
- Fetch latency

**Implementation with Spring Boot Actuator:**

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

```java
@Component
public class KafkaMetrics {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    public void recordProducerMetrics(SendResult<String, Object> result) {
        meterRegistry.counter("kafka.producer.messages.sent",
            "topic", result.getRecordMetadata().topic()
        ).increment();
        
        meterRegistry.timer("kafka.producer.latency",
            "topic", result.getRecordMetadata().topic()
        ).record(Duration.ofMillis(result.getRecordMetadata().timestamp()));
    }
    
    public void recordConsumerLag(String topic, int partition, long lag) {
        meterRegistry.gauge("kafka.consumer.lag",
            Tags.of("topic", topic, "partition", String.valueOf(partition)),
            lag
        );
    }
}
```

**Monitoring tools:**
- Prometheus + Grafana
- Kafka Manager / CMAK
- Burrow (LinkedIn's consumer lag checker)
- Confluent Control Center
- Datadog / New Relic


## 8. Advanced Topics

### Q19: Explain Kafka Streams and when to use it

**Answer:**

**Kafka Streams** is a client library for building real-time, scalable, fault-tolerant stream processing applications.

**Use cases:**
- Real-time data transformation
- Aggregations and windowing
- Joining streams
- Stateful processing

**Example: Word count:**

```java
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
    
    @Bean
    public KStream<String, String> wordCountStream(StreamsBuilder builder) {
        
        // Input stream
        KStream<String, String> textLines = builder.stream("input-topic");
        
        // Process: split, group, count
        KTable<String, Long> wordCounts = textLines
            .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word)
            .count(Materialized.as("word-counts-store"));
        
        // Output to topic
        wordCounts.toStream()
            .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        
        return textLines;
    }
}
```

**Advanced example: Aggregation with windowing:**

```java
@Bean
public KStream<String, OrderEvent> processOrders(StreamsBuilder builder) {
    
    KStream<String, OrderEvent> orders = builder.stream("orders");
    
    // Aggregate sales by user in 1-hour windows
    KTable<Windowed<String>, Double> salesByUser = orders
        .groupBy((key, order) -> order.getUserId())
        .windowedBy(TimeWindows.of(Duration.ofHours(1)))
        .aggregate(
            () -> 0.0,
            (userId, order, total) -> total + order.getAmount(),
            Materialized.with(Serdes.String(), Serdes.Double())
        );
    
    // Find users with sales > $1000
    salesByUser
        .filter((windowedUserId, total) -> total > 1000)
        .toStream()
        .to("high-value-users");
    
    return orders;
}
```


### Q20: How does Kafka compare with RabbitMQ/ActiveMQ?

**Answer:**

**Kafka:**
- **Use case**: High-throughput event streaming, log aggregation
- **Architecture**: Distributed commit log
- **Message retention**: Persistent (configurable retention)
- **Ordering**: Guaranteed within partition
- **Throughput**: Very high (millions/sec)
- **Latency**: Low to medium
- **Scalability**: Excellent horizontal scaling
- **Replay**: Yes, consumers can replay messages

**RabbitMQ:**
- **Use case**: Traditional message queueing, task distribution
- **Architecture**: Message broker with queues
- **Message retention**: Deleted after consumption
- **Ordering**: FIFO in queue
- **Throughput**: Medium (thousands/sec)
- **Latency**: Very low
- **Scalability**: Good, but more complex
- **Replay**: No, messages deleted after ack

**When to use Kafka:**
- High-throughput data pipelines
- Event sourcing
- Log aggregation
- Stream processing
- Need message replay
- Multiple consumers need same data

**When to use RabbitMQ:**
- Traditional request-response
- Complex routing requirements
- Low latency is critical
- Task queues with work distribution
- Don't need message replay
- Smaller scale


## Summary of Critical Points

1. **Ordering**: Only guaranteed within a partition
2. **Partitioning**: Use keys to ensure related messages go to same partition
3. **Consumer groups**: Each partition consumed by one consumer in group
4. **Replication**: For fault tolerance, use replication factor >= 3
5. **Acknowledgment**: Use acks=all for no message loss
6. **Idempotence**: Enable for exactly-once producer semantics
7. **Consumer lag**: Monitor and tune concurrency/batch size
8. **Error handling**: Implement retry logic and dead letter queues
9. **Monitoring**: Track broker, producer, and consumer metrics
10. **Security**: Use SSL/TLS and SASL for production

---

Remember: These scenarios test your understanding of Kafka's fundamentals, your ability to troubleshoot issues, and your architectural decision-making skills. Always explain the trade-offs in your solutions!