# Kafka Code - Line by Line Explanation

## 📦 Part 1: Kafka Consumer Service

### Package and Imports

```java
package com.example.kafka.consumer;
```
- Declares this class belongs to the `consumer` package
- Helps organize code by functionality

```java
import com.example.kafka.model.OrderEvent;
import com.example.kafka.model.UserEvent;
```
- Imports custom data models (UserEvent and OrderEvent)
- These are the types of messages we'll receive

```java
import com.fasterxml.jackson.databind.ObjectMapper;
```
- Imports Jackson library for converting JSON to Java objects
- Like a translator between JSON text and Java objects

```java
import lombok.extern.slf4j.Slf4j;
```
- Lombok annotation that automatically creates a logger variable
- Allows us to write log messages (like print statements for production)

```java
import org.apache.kafka.clients.consumer.ConsumerRecord;
```
- Imports the ConsumerRecord class
- Represents a single message from Kafka with metadata

```java
import org.springframework.kafka.annotation.KafkaListener;
```
- Annotation that marks methods as Kafka message listeners
- Spring automatically calls these methods when messages arrive

```java
import org.springframework.kafka.support.Acknowledgment;
```
- Allows manual acknowledgment of messages
- Like confirming "Yes, I received and processed this message"

```java
import org.springframework.kafka.support.KafkaHeaders;
```
- Constants for Kafka header names
- Headers contain metadata like topic, partition, offset

```java
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
```
- Annotations to extract specific parts of the message
- `@Payload` = the actual message content
- `@Header` = metadata about the message

```java
import org.springframework.stereotype.Service;
```
- Marks this class as a Spring service component
- Spring will automatically create and manage this class

```java
import java.util.List;
```
- Java's List interface for collections
- Used for batch processing multiple messages

---

### Class Declaration

```java
@Service
```
- Spring annotation marking this as a service layer component
- Spring creates one instance and manages its lifecycle

```java
@Slf4j
```
- Lombok annotation that creates a logger named `log`
- Equivalent to: `private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);`

```java
public class KafkaConsumerService {
```
- Public class containing all consumer methods
- Other classes can use this service

```java
private final ObjectMapper objectMapper = new ObjectMapper();
```
- Creates a JSON converter object
- `final` means it's initialized once and never changes
- Used to convert JSON strings to Java objects

---

### Method 1: Basic Consumer

```java
@KafkaListener(
```
- Annotation that makes this method listen to Kafka topics
- Spring calls this method automatically when messages arrive

```java
    topics = "${kafka.topics.user-events}",
```
- Specifies which topic to listen to
- `${}` reads value from application.properties file
- Example: `kafka.topics.user-events=user-events-topic`

```java
    groupId = "${spring.kafka.consumer.group-id}",
```
- Consumer group ID for load balancing
- Multiple consumers with same group ID share the workload
- Each message goes to only ONE consumer in the group

```java
    containerFactory = "kafkaListenerContainerFactory"
```
- Specifies configuration for this listener
- Factory creates the container that manages this consumer

```java
)
public void consumeUserEvent(
```
- Public method that processes user events
- Spring calls this when a message arrives

```java
    @Payload UserEvent userEvent,
```
- `@Payload` extracts the message body
- Automatically converts JSON to UserEvent object
- This is the actual data we want to process

```java
    Acknowledgment acknowledgment
```
- Object to manually confirm message receipt
- We call `acknowledge()` when processing is complete

```java
) {
    try {
```
- Try-catch block to handle errors gracefully
- If something goes wrong, we catch the exception

```java
        log.info("Consumed User Event: {}", userEvent);
```
- Logs the received event
- `{}` is a placeholder replaced by userEvent
- Like: `print(f"Consumed User Event: {userEvent}")`

```java
        processUserEvent(userEvent);
```
- Calls helper method to do actual business logic
- Separates message reception from processing

```java
        acknowledgment.acknowledge();
```
- Tells Kafka "I successfully processed this message"
- Kafka marks message as consumed and won't resend it

```java
    } catch (Exception e) {
```
- Catches any error that occurs during processing

```java
        log.error("Error processing user event: {}", e.getMessage(), e);
```
- Logs error message with full stack trace
- First `{}` = error message, `e` = full exception details

```java
        // Don't acknowledge - message will be reprocessed
```
- Comment explaining: no acknowledgment = Kafka will retry
- Message stays "unprocessed" and will be delivered again

```java
    }
}
```
- End of method and try-catch block

---

### Method 2: Consumer with Metadata

```java
@KafkaListener(
    topics = "${kafka.topics.order-events}",
    groupId = "${spring.kafka.consumer.group-id}",
    containerFactory = "kafkaListenerContainerFactory"
)
```
- Same listener annotation but for order events

```java
public void consumeOrderEventWithMetadata(
    ConsumerRecord<String, String> record,
```
- Receives raw ConsumerRecord instead of deserialized object
- `<String, String>` = Key type is String, Value type is String
- Gives access to all message details, not just the payload

```java
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
```
- Extracts partition number from message headers
- Partitions are like "lanes" in Kafka for parallel processing

```java
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
```
- Extracts the topic name from headers
- Useful when listening to multiple topics

```java
    @Header(KafkaHeaders.OFFSET) long offset,
```
- Extracts message offset (position number in partition)
- Each message has a unique offset within its partition

```java
    Acknowledgment acknowledgment
) {
    try {
        log.info("Consumed from Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
```
- Logs detailed metadata about the message
- Three placeholders `{}` filled by topic, partition, offset

```java
        log.info("Key: {}, Value: {}", record.key(), record.value());
```
- Logs message key and value
- Keys are used for partitioning and ordering

```java
        OrderEvent orderEvent = objectMapper.readValue(record.value(), OrderEvent.class);
```
- Manually converts JSON string to OrderEvent object
- `record.value()` = JSON string
- `OrderEvent.class` = target type

```java
        processOrderEvent(orderEvent);
        acknowledgment.acknowledge();
```
- Process the event and acknowledge successful completion

```java
    } catch (Exception e) {
        log.error("Error processing order event: {}", e.getMessage(), e);
    }
}
```
- Error handling (note: no acknowledgment on error)

---

### Method 3: Batch Consumer

```java
@KafkaListener(
    topics = "${kafka.topics.user-events}",
    groupId = "batch-consumer-group",
```
- Different group ID means this runs independently
- Won't compete with single-message consumers

```java
    containerFactory = "batchListenerContainerFactory"
```
- Special factory configured for batch processing
- Collects multiple messages before calling this method

```java
)
public void consumeUserEventsBatch(
    List<UserEvent> userEvents,
```
- Receives a LIST of events, not just one
- Batch size configured in factory (e.g., 100 messages)

```java
    Acknowledgment acknowledgment
) {
    try {
        log.info("Consumed batch of {} user events", userEvents.size());
```
- Logs how many events in this batch

```java
        for (UserEvent event : userEvents) {
            processUserEvent(event);
        }
```
- Loops through each event in the batch
- Processes them one by one

```java
        acknowledgment.acknowledge();
```
- Single acknowledgment for entire batch
- All messages marked as processed together

```java
    } catch (Exception e) {
        log.error("Error processing user events batch: {}", e.getMessage(), e);
    }
}
```
- If ANY message fails, ENTIRE batch fails and retries

---

### Method 4: Multiple Topics Consumer

```java
@KafkaListener(
    topics = {"${kafka.topics.user-events}", "${kafka.topics.order-events}"},
```
- Array with two topics
- This consumer listens to BOTH topics simultaneously

```java
    groupId = "multi-topic-consumer-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeFromMultipleTopics(
    @Payload String message,
```
- Receives raw String (not deserialized object)
- We'll manually determine type based on topic

```java
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    Acknowledgment acknowledgment
) {
    try {
        log.info("Consumed from topic: {}, Message: {}", topic, message);
```
- Logs which topic the message came from

```java
        if (topic.contains("user-events")) {
```
- Checks if topic name contains "user-events"
- Routes message to appropriate handler

```java
            UserEvent userEvent = objectMapper.readValue(message, UserEvent.class);
            processUserEvent(userEvent);
```
- Converts JSON to UserEvent and processes it

```java
        } else if (topic.contains("order-events")) {
            OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);
            processOrderEvent(orderEvent);
        }
```
- Handles order events differently

```java
        acknowledgment.acknowledge();
    } catch (Exception e) {
        log.error("Error processing message from multiple topics: {}", e.getMessage(), e);
    }
}
```
- Acknowledge and handle errors

---

### Method 5: Pattern Consumer

```java
@KafkaListener(
    topicPattern = ".*-events-topic",
```
- Regular expression pattern instead of specific topic names
- `.*` = match any characters
- `-events-topic` = must end with this
- Example matches: "user-events-topic", "order-events-topic", "payment-events-topic"

```java
    groupId = "pattern-consumer-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeWithPattern(
    @Payload String message,
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    Acknowledgment acknowledgment
) {
    try {
        log.info("Pattern consumer - Topic: {}, Message: {}", topic, message);
        acknowledgment.acknowledge();
    } catch (Exception e) {
        log.error("Error in pattern consumer: {}", e.getMessage(), e);
    }
}
```
- Dynamically listens to any topic matching the pattern
- Useful when new topics are added without code changes

---

### Method 6: Specific Partition Consumer

```java
@KafkaListener(
    topicPartitions = @org.springframework.kafka.annotation.TopicPartition(
```
- Uses TopicPartition annotation for fine-grained control

```java
        topic = "${kafka.topics.user-events}",
        partitions = {"0", "1"}
```
- Only consumes from partitions 0 and 1
- Ignores messages in other partitions (2, 3, 4, etc.)

```java
    ),
    groupId = "partition-specific-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeFromSpecificPartitions(
    @Payload UserEvent userEvent,
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
    Acknowledgment acknowledgment
) {
    try {
        log.info("Consumed from partition {}: {}", partition, userEvent);
```
- Logs which partition message came from

```java
        processUserEvent(userEvent);
        acknowledgment.acknowledge();
    } catch (Exception e) {
        log.error("Error consuming from specific partition: {}", e.getMessage(), e);
    }
}
```
- Process and acknowledge as usual

---

### Method 7: Concurrent Consumer

```java
@KafkaListener(
    topics = "${kafka.topics.order-events}",
    groupId = "concurrent-consumer-group",
    containerFactory = "kafkaListenerContainerFactory",
    concurrency = "5"
```
- `concurrency = "5"` creates 5 parallel consumer threads
- Each thread independently reads from different partitions
- 5x processing speed (if you have enough partitions)

```java
)
public void consumeWithConcurrency(
    @Payload OrderEvent orderEvent,
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
    Acknowledgment acknowledgment
) {
    try {
        log.info("Thread {}: Processing order from partition {}: {}", 
            Thread.currentThread().getName(), partition, orderEvent.getOrderId());
```
- Logs which thread is processing
- `Thread.currentThread().getName()` = thread identifier
- Shows parallel processing in action

```java
        processOrderEvent(orderEvent);
        acknowledgment.acknowledge();
    } catch (Exception e) {
        log.error("Error in concurrent consumer: {}", e.getMessage(), e);
    }
}
```

---

### Helper Methods

```java
private void processUserEvent(UserEvent userEvent) {
```
- Private helper method for business logic
- Only this class can call it

```java
    log.info("Processing user event: Type={}, UserId={}", 
        userEvent.getEventType(), userEvent.getUserId());
```
- Logs event type and user ID

```java
    switch (userEvent.getEventType()) {
```
- Switch statement based on event type
- Different logic for different event types

```java
        case "CREATED":
            log.info("New user created: {}", userEvent.getUsername());
            break;
```
- If event type is "CREATED", log username
- `break` exits the switch statement

```java
        case "UPDATED":
            log.info("User updated: {}", userEvent.getUsername());
            break;
        case "DELETED":
            log.info("User deleted: {}", userEvent.getUserId());
            break;
        case "LOGIN":
            log.info("User login: {} from {}", userEvent.getUsername(), userEvent.getIpAddress());
            break;
```
- Similar handling for other event types

```java
        default:
            log.warn("Unknown event type: {}", userEvent.getEventType());
```
- If event type doesn't match any case, log warning
- `warn` = less severe than error

```java
    }
}
```

```java
private void processOrderEvent(OrderEvent orderEvent) {
    log.info("Processing order event: Status={}, OrderId={}, Amount={}",
        orderEvent.getStatus(), orderEvent.getOrderId(), orderEvent.getAmount());
    
    switch (orderEvent.getStatus()) {
        case "CREATED":
            log.info("New order created: {} for user {}", 
                orderEvent.getOrderId(), orderEvent.getUserId());
            break;
        case "CONFIRMED":
            log.info("Order confirmed: {}", orderEvent.getOrderId());
            break;
        case "SHIPPED":
            log.info("Order shipped: {} to {}", 
                orderEvent.getOrderId(), orderEvent.getShippingAddress());
            break;
        case "DELIVERED":
            log.info("Order delivered: {}", orderEvent.getOrderId());
            break;
        case "CANCELLED":
            log.info("Order cancelled: {}", orderEvent.getOrderId());
            break;
        default:
            log.warn("Unknown order status: {}", orderEvent.getStatus());
    }
}
```
- Similar pattern for order events
- Different statuses for order lifecycle

---

## 📦 Part 2: Kafka Controller (REST API)

### Package and Imports

```java
package com.example.kafka.controller;
```
- Controller package for REST endpoints

```java
import com.example.kafka.model.OrderEvent;
import com.example.kafka.model.UserEvent;
```
- Import event models

```java
import com.example.kafka.producer.KafkaProducerService;
```
- Import producer service to send messages

```java
import lombok.extern.slf4j.Slf4j;
```
- Logging annotation

```java
import org.springframework.beans.factory.annotation.Autowired;
```
- Annotation for dependency injection
- Spring automatically provides the required object

```java
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
```
- HTTP status codes and response wrapper

```java
import org.springframework.web.bind.annotation.*;
```
- REST controller annotations (@RestController, @GetMapping, @PostMapping, etc.)

```java
import java.math.BigDecimal;
```
- For precise decimal numbers (money amounts)

```java
import java.time.LocalDateTime;
```
- For date and time

```java
import java.util.HashMap;
import java.util.Map;
```
- Map data structure for JSON responses

```java
import java.util.UUID;
```
- Generates unique IDs

---

### Class Declaration

```java
@RestController
```
- Marks this as a REST API controller
- Methods return JSON automatically

```java
@RequestMapping("/api/kafka")
```
- Base path for all endpoints in this controller
- All URLs start with /api/kafka

```java
@Slf4j
public class KafkaController {
```

```java
    @Autowired
    private KafkaProducerService producerService;
```
- `@Autowired` tells Spring to inject the producer service
- Spring finds and provides the instance automatically

---

### Endpoint 1: Send User Event

```java
@PostMapping("/user-event")
```
- HTTP POST method
- Full URL: POST /api/kafka/user-event

```java
public ResponseEntity<Map<String, String>> sendUserEvent(@RequestBody UserEvent userEvent) {
```
- `ResponseEntity` = HTTP response with status code and body
- `Map<String, String>` = JSON object with string key-value pairs
- `@RequestBody` = reads JSON from request body and converts to UserEvent

```java
    try {
        if (userEvent.getTimestamp() == null) {
            userEvent.setTimestamp(LocalDateTime.now());
        }
```
- If timestamp not provided, set it to current time

```java
        if (userEvent.getUserId() == null) {
            userEvent.setUserId(UUID.randomUUID().toString());
        }
```
- If userId not provided, generate random unique ID

```java
        producerService.sendUserEventWithCallback(userEvent);
```
- Call producer service to send message to Kafka

```java
        Map<String, String> response = new HashMap<>();
```
- Create empty map for JSON response

```java
        response.put("status", "success");
        response.put("message", "User event sent successfully");
        response.put("userId", userEvent.getUserId());
```
- Add key-value pairs to response
- Will become: `{"status": "success", "message": "...", "userId": "..."}`

```java
        return ResponseEntity.ok(response);
```
- Return HTTP 200 OK with JSON body

```java
    } catch (Exception e) {
        log.error("Error sending user event: {}", e.getMessage(), e);
        Map<String, String> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
```
- If error occurs, return HTTP 500 with error details

```java
    }
}
```

---

### Endpoint 2: Send Order Event

```java
@PostMapping("/order-event")
public ResponseEntity<Map<String, String>> sendOrderEvent(@RequestBody OrderEvent orderEvent) {
    try {
        if (orderEvent.getOrderId() == null) {
            orderEvent.setOrderId("ORD-" + UUID.randomUUID().toString());
        }
```
- Generate order ID with "ORD-" prefix

```java
        if (orderEvent.getOrderDate() == null) {
            orderEvent.setOrderDate(LocalDateTime.now());
        }
        
        producerService.sendOrderEvent(orderEvent);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Order event sent successfully");
        response.put("orderId", orderEvent.getOrderId());
        
        return ResponseEntity.ok(response);
    } catch (Exception e) {
        log.error("Error sending order event: {}", e.getMessage(), e);
        Map<String, String> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
```
- Similar structure to user event endpoint

---

### Endpoint 3: Generate Sample User Event

```java
@GetMapping("/generate/user-event")
```
- HTTP GET method (no request body needed)
- Full URL: GET /api/kafka/generate/user-event

```java
public ResponseEntity<Map<String, String>> generateUserEvent() {
    try {
        UserEvent userEvent = UserEvent.builder()
```
- Uses builder pattern (from Lombok @Builder annotation)
- Cleaner way to construct objects with many fields

```java
            .userId(UUID.randomUUID().toString())
            .eventType("CREATED")
            .username("user_" + System.currentTimeMillis())
```
- Username with timestamp ensures uniqueness

```java
            .email("user@example.com")
            .timestamp(LocalDateTime.now())
            .ipAddress("192.168.1.1")
            .metadata("Generated from REST API")
            .build();
```
- `.build()` creates the final UserEvent object

```java
        producerService.sendUserEventWithCallback(userEvent);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Sample user event generated and sent");
        response.put("userId", userEvent.getUserId());
        
        return ResponseEntity.ok(response);
    } catch (Exception e) {
        log.error("Error generating user event: {}", e.getMessage(), e);
        Map<String, String> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
```

---

### Endpoint 4: Generate Sample Order Event

```java
@GetMapping("/generate/order-event")
public ResponseEntity<Map<String, String>> generateOrderEvent() {
    try {
        OrderEvent orderEvent = OrderEvent.builder()
            .orderId("ORD-" + UUID.randomUUID().toString())
            .userId(UUID.randomUUID().toString())
            .status("CREATED")
            .amount(new BigDecimal("299.99"))
```
- `BigDecimal` for precise money calculations
- Avoids floating-point rounding errors

```java
            .currency("USD")
            .itemCount(3)
            .orderDate(LocalDateTime.now())
            .shippingAddress("123 Main St, City, Country")
            .paymentMethod("CREDIT_CARD")
            .build();
        
        producerService.sendOrderEvent(orderEvent);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Sample order event generated and sent");
        response.put("orderId", orderEvent.getOrderId());
        
        return ResponseEntity.ok(response);
    } catch (Exception e) {
        log.error("Error generating order event: {}", e.getMessage(), e);
        Map<String, String> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
```

---

### Endpoint 5: Load Test

```java
@GetMapping("/load-test")
public ResponseEntity<Map<String, Object>> loadTest(
    @RequestParam(defaultValue = "100") int count
```
- `@RequestParam` reads query parameter from URL
- Example: /api/kafka/load-test?count=500
- If not provided, defaults to 100

```java
) {
    try {
        long startTime = System.currentTimeMillis();
```
- Record start time in milliseconds

```java
        for (int i = 0; i < count; i++) {
```
- Loop 'count' times

```java
            UserEvent userEvent = UserEvent.builder()
                .userId(UUID.randomUUID().toString())
                .eventType("CREATED")
                .username("user_" + i)
                .email("user" + i + "@example.com")
                .timestamp(LocalDateTime.now())
                .ipAddress("192.168.1." + (i % 255))
```
- `i % 255` = cycles through 0-254
- Creates different IP addresses

```java
                .metadata("Load test event " + i)
                .build();
            
            producerService.sendUserEvent(userEvent);
        }
```
- Send each event

```java
        long endTime = System.currentTimeMillis();
```
- Record end time

```java
        Map<String, Object> response = new HashMap<>();
```
- `Map<String, Object>` allows mixed value types (String, int, double)

```java
        response.put("status", "success");
        response.put("message", count + " events sent successfully");
        response.put("count", count);
        response.put("timeTaken", (endTime - startTime) + "ms");
        response.put("throughput", (count * 1000.0) / (endTime - startTime) + " events/sec");
```
- Calculate throughput: events/second
- `count * 1000.0` converts milliseconds to seconds

```java
        return ResponseEntity.ok(response);
    } catch (Exception e) {
        log.error("Error in load test: {}", e.getMessage(), e);
        Map<String, Object> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
```

---

### Endpoint 6: Health Check

```java
@GetMapping("/health")
public ResponseEntity<Map<String, String>> health() {
    Map<String, String> response = new HashMap<>();
    response.put("status", "UP");
    response.put("message", "Kafka service is running");
    return ResponseEntity.ok(response);
}
```
- Simple endpoint to check if service is running
- No complex logic, just returns success

---

## 📦 Part 3: Error Handler

### Package and Imports

```java
package com.example.kafka.errorhandler;
```

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
```
- Kafka consumer classes

```java
import org.springframework.kafka.listener.CommonErrorHandler;
```
- Interface for custom error handling

```java
import org.springframework.kafka.listener.MessageListenerContainer;
```
- Container that manages consumer lifecycle

```java
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
```
- Message building utilities

```java
import org.springframework.stereotype.Component;
```
- Marks this as a Spring component

---

### Class Declaration

```java
@Component
```
- Spring creates and manages this component

```java
@Slf4j
public class KafkaErrorHandler implements CommonErrorHandler {
```
- Implements error handler interface
- Spring calls these methods when errors occur

---

### Method 1: Handle Batch Errors

```java
@Override
public void handleBatch(Exception thrownException,
```
- Override means implementing interface method
- Called when batch processing fails

```java
                       org.apache.kafka.clients.consumer.ConsumerRecords<?, ?> data,
```
- Collection of records that failed
- `<?, ?>` = generic types (key and value can be anything)

```java
                       Consumer<?, ?> consumer,
```
- The Kafka consumer instance

```java
                       MessageListenerContainer container,
```
- Container managing the consumer

```java
                       Runnable invokeListener) {
```
- Callback to retry processing

```java
    log.error("Error in batch processing. Total records: {}, Error: {}", 
        data.count(), thrownException.getMessage());
```
- Log how many records failed

```java
    data.forEach(record -> {
        log.error("Failed record - Topic: {}, Partition: {}, Offset: {}, Key: {}", 
            record.topic(), record.partition(), record.offset(), record.key());
    });
```
- Loop through each failed record
- Log details for debugging

```java
    if (shouldRetry(thrownException)) {
        log.info("Retrying batch processing");
        invokeListener.run();
```
- If error is recoverable, retry the entire batch
- `invokeListener.run()` = try processing again

```java
    } else {
        log.error("Moving batch to Dead Letter Topic");
        // Send to DLT logic here
    }
}
```
- If not recoverable, send to dead letter topic

---

### Method 2: Handle Single Record Errors

```java
@Override
public boolean handleOne(Exception thrownException,
                        ConsumerRecord<?, ?> record,
                        Consumer<?, ?> consumer,
                        MessageListenerContainer container) {
```
- Called when single record processing fails
- Returns boolean: true = retry, false = skip

```java
    log.error("Error processing record - Topic: {}, Partition: {}, Offset: {}, Error: {}", 
        record.topic(), record.partition(), record.offset(), thrownException.getMessage());
    
    if (isRecoverableError(thrownException)) {
        log.info("Recoverable error, will retry");
        return true; // Retry
```
- Return true tells Kafka to retry this message

```java
    } else {
        log.error("Non-recoverable error, sending to DLT");
        sendToDeadLetterTopic(record, thrownException);
        return false; // Don't retry, move on
```
- Return false skips this message and moves to next

```java
    }
}
```

---

### Method 3: Check if Error is Recoverable

```java
private boolean isRecoverableError(Exception exception) {
```
- Determines if we should retry

```java
    return exception instanceof org.springframework.dao.TransientDataAccessException
```
- `instanceof` checks if exception is of this type
- TransientDataAccessException = temporary database issue

```java
        || exception instanceof java.net.SocketTimeoutException
```
- `||` = OR operator
- Network timeout = recoverable

```java
        || exception instanceof org.springframework.kafka.KafkaException;
```
- General Kafka exception = might be recoverable

```java
}
```
- If any condition is true, returns true

---

### Method 4: Should Retry

```java
private boolean shouldRetry(Exception exception) {
    return isRecoverableError(exception);
}
```
- Simple wrapper around isRecoverableError
- Could add additional logic later (retry count, etc.)

---

### Method 5: Send to Dead Letter Topic

```java
private void sendToDeadLetterTopic(ConsumerRecord<?, ?> record, Exception exception) {
    try {
        log.info("Sending record to DLT - Topic: {}, Partition: {}, Offset: {}", 
            record.topic(), record.partition(), record.offset());
```
- Log DLT send attempt

```java
        Message<?> message = MessageBuilder
```
- Create new message with error details

```java
            .withPayload(record.value())
```
- Use original message content as payload

```java
            .setHeader(KafkaHeaders.TOPIC, "dead-letter-topic")
```
- Set destination topic

```java
            .setHeader("original-topic", record.topic())
            .setHeader("original-partition", record.partition())
            .setHeader("original-offset", record.offset())
```
- Add metadata about where message came from

```java
            .setHeader("error-message", exception.getMessage())
            .setHeader("error-class", exception.getClass().getName())
```
- Add error details for debugging

```java
            .setHeader("timestamp", System.currentTimeMillis())
```
- Record when error occurred

```java
            .build();
```
- Create final message

```java
        // Send to DLT (would need KafkaTemplate injected)
        // kafkaTemplate.send(message);
```
- Comment: actual send code would go here
- Needs KafkaTemplate dependency

```java
        log.info("Record sent to DLT successfully");
        
    } catch (Exception e) {
        log.error("Failed to send record to DLT: {}", e.getMessage(), e);
    }
}
```
- Error handling for DLT send (meta error handling!)

---

## Summary

This code creates a complete Kafka system with:

1. **7 different consumer patterns** for various use cases
2. **6 REST endpoints** to send messages and test the system
3. **Comprehensive error handling** with retry logic and dead letter topic

Each component is well-structured, uses Spring Boot best practices, and includes detailed logging for monitoring and debugging!
