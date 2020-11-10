package org.example.aws;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@Slf4j
public class MdcDeleteAwsTest {

  private static final String KEY = "correlationId";

  private static final Executor executor = new MdcPropagationPool(10);

  private static SqsAsyncClient sqsClient;

  /**
   * Gets the a queue URL from its name.
   * @param queueName The queue name.
   * @return The queue URL if found. An exception if not.
   */
  public static String getQueueUrl(String queueName) {
    log.info("Trying to find queue {}", queueName);
    return sqsClient.getQueueUrl(builder -> builder.queueName(queueName))
        .join().queueUrl();
  }


  /**
   * Gets the URL of a queue. If it doesn't exist, try to create one and return its URL.
   * @param queueName The queue name.
   * @return The queue URL for this queue name.
   */
  public static String getOrCreateQueue(String queueName) {
    String queueUrl = null;
    try {
      queueUrl = getQueueUrl(queueName);
      log.info("Queue {} found at URL {}", queueName, queueUrl);
    } catch (CompletionException e) {
      if (e.getCause() instanceof QueueDoesNotExistException) {
        log.info("Queue {} does not exist. Creating it", queueName);
        queueUrl = sqsClient.createQueue(builder -> builder.queueName(queueName))
            .join().queueUrl();
        log.info("Queue {} created at {}", queueName, queueUrl);
      }
    }
    return queueUrl;
  }

  public static ReceiveMessageRequest receiveMessageRequest(String queueUrl) {
    return ReceiveMessageRequest.builder()
        .queueUrl(queueUrl).messageAttributeNames("All")
        .maxNumberOfMessages(10)
        .waitTimeSeconds(3)
        .visibilityTimeout(30).build();
  }

  public static void main(String[] args) throws URISyntaxException, InterruptedException {
    MDC.put(KEY, "myValue");

    CompletableFuture.runAsync(MdcDeleteAwsTest::doWork, executor)
        .whenCompleteAsync((r, e) -> log.info("Finished doing work with correlation {}", MDC.get(KEY)), executor);

    sqsClient = SqsAsyncClient.builder()
        .asyncConfiguration(
            builder ->
                builder.advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    executor))
        .endpointOverride(new URI("http://localhost:4566"))
        .build();

    String queueUrl = getOrCreateQueue("test-queue");

    CompletableFuture.supplyAsync(() -> sqsClient.receiveMessage(receiveMessageRequest(queueUrl)).join(), executor)
        .whenComplete((response, error) -> {
          log.info("Got blocking receive response with {} messages and correlation ", response.messages().size(), MDC.get(KEY));
        });

    sqsClient.receiveMessage(receiveMessageRequest(queueUrl)).whenComplete((response, error) -> {
      log.info("Received asynchronous receive response with {} messages and correlation", response.messages().size(), MDC.get(KEY));
    });

    Thread.sleep(TimeUnit.SECONDS.toMillis(5));

    CompletableFuture.supplyAsync(() -> sqsClient.receiveMessage(receiveMessageRequest(queueUrl)).join(), executor)
        .whenComplete((response, error) -> {
          log.info("Got blocking receive response after asynchronous with {} messages and correlation ", response.messages().size(), MDC.get(KEY));
        });

  }

  private static void doWork() {
    try {
      log.info("Sleeping for 1 second");
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    } catch (InterruptedException e) {
      log.error("Interrupted while sleeping", e);
    }
  }
}
