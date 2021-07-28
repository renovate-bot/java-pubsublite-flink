/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pubsublite.flink;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.flink.sink.PerServerPublisherCache;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig.DeliveryRequirement;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig.Capacity;
import com.google.cloud.pubsublite.proto.Topic.RetentionConfig;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

public class IntegrationTest {
  private static final String ZONE = "us-central1-b";
  //  private static final String TOPIC = "flink-test";
  //  private static final String SUBSCRIPTION = "flink-test-sub";
  private static final String TOPIC = "flink-integration-test-topic";
  private static final String SUBSCRIPTION = "flink-integration-test-subscription";
  private static final UUID uuid = UUID.randomUUID();

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  private static ProjectNumber projectNumber() {
    // return ProjectNumber.of(856003786687L);
    return ProjectNumber.of(Long.parseLong(System.getenv("GOOGLE_CLOUD_PROJECT_NUMBER")));
  }

  private static TopicPath topicPath() {
    return TopicPath.newBuilder()
        .setLocation(CloudZone.parse(ZONE))
        .setProject(projectNumber())
        .setName(TopicName.of(TOPIC))
        .build();
  }

  private static SubscriptionPath subscriptionPath() {
    return SubscriptionPath.newBuilder()
        .setLocation(CloudZone.parse(ZONE))
        .setProject(projectNumber())
        .setName(SubscriptionName.of(SUBSCRIPTION))
        .build();
  }

  private static PubsubLiteSourceSettings.Builder<String> sourceSettings() {
    return PubsubLiteSourceSettings.builder(
            PubsubLiteDeserializationSchema.dataOnly(new SimpleStringSchema()))
        .setSubscriptionPath(subscriptionPath())
        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
        .setFlowControlSettings(
            FlowControlSettings.builder()
                .setBytesOutstanding(100000)
                .setMessagesOutstanding(100)
                .build());
  }

  private static PubsubLiteSinkSettings.Builder<String> sinkSettings() {
    return PubsubLiteSinkSettings.builder(
            PubsubLiteSerializationSchema.dataOnly(new SimpleStringSchema()))
        .setTopicPath(topicPath());
  }

  private static Publisher<MessageMetadata> getPublisher() {
    return PerServerPublisherCache.getOrCreate(sinkSettings().build().getPublisherConfig());
  }

  private static AdminClient getAdminClient() {
    return sourceSettings().build().getAdminClient();
  }

  private static Message messageFromString(String i) {
    SimpleStringSchema s = new SimpleStringSchema();
    return Message.builder().setData(ByteString.copyFrom(s.serialize(i))).build();
  }

  private static String computeTestPrefix(String testName) {
    return String.format("%s-%s-", testName, uuid.toString());
  }

  private static List<String> prefixedStrings(IntStream stream, String prefix) {
    return stream.mapToObj(i -> prefix + i).collect(Collectors.toList());
  }

  @BeforeClass
  public static void verifyResources() throws Exception {
    getAdminClient()
        .createTopic(
            Topic.newBuilder()
                .setName(topicPath().toString())
                .setPartitionConfig(
                    PartitionConfig.newBuilder()
                        .setCount(2)
                        .setCapacity(
                            Capacity.newBuilder()
                                .setPublishMibPerSec(4)
                                .setSubscribeMibPerSec(4)
                                .build())
                        .build())
                .setRetentionConfig(
                    RetentionConfig.newBuilder().setPerPartitionBytes(32212254720L).build())
                .build())
        .get();
    getAdminClient()
        .createSubscription(
            Subscription.newBuilder()
                .setTopic(topicPath().toString())
                .setDeliveryConfig(
                    DeliveryConfig.newBuilder()
                        .setDeliveryRequirement(DeliveryRequirement.DELIVER_IMMEDIATELY)
                        .build())
                .setName(subscriptionPath().toString())
                .buildPartial())
        .get();
    try (AdminClient client = getAdminClient()) {
      assertThat(client.getTopic(topicPath()).get().getPartitionConfig().getCount()).isEqualTo(2);
      assertThat(client.getSubscription(subscriptionPath()).get().getTopic())
          .isEqualTo(topicPath().toString());
    }
  }

  @Test
  public void testSource() throws Exception {
    final String prefix = computeTestPrefix("testSource");
    List<String> strings = prefixedStrings(IntStream.range(0, 100), prefix);

    CollectSink sink = new CollectSink(prefix);
    Publisher<MessageMetadata> publisher = getPublisher();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(sink);
    JobClient client = env.executeAsync();

    ApiFutures.allAsList(
            strings.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    while (sink.values().size() < 100) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(sink.values()).containsExactlyElementsIn(strings);
    client.cancel().get();
  }

  @Test
  public void testBoundedSource() throws Exception {
    final String prefix = computeTestPrefix("testBoundedSource");
    List<String> strings = prefixedStrings(IntStream.range(0, 100), prefix);

    CollectSink sink = new CollectSink(prefix);
    Publisher<MessageMetadata> publisher = getPublisher();

    // A condition that accepts one message.
    PartitionFinishedCondition.Factory condition =
        (path, partition) ->
            (PartitionFinishedCondition)
                message -> {
                  if (message.message().data().toStringUtf8().startsWith(prefix)) {
                    return Result.FINISH_AFTER;
                  }
                  return Result.CONTINUE;
                };

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    env.fromSource(
            new PubsubLiteSource<>(
                sourceSettings()
                    .setBoundedness(Boundedness.BOUNDED)
                    .setPartitionFinishedCondition(condition)
                    .build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(sink);
    JobClient client = env.executeAsync();

    ApiFutures.allAsList(
            strings.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    client.getJobExecutionResult().get();
    // One message per partition.
    assertThat(sink.values()).hasSize(2);
  }

  @Test
  public void testSourceWithFailure() throws Exception {
    final String prefix = computeTestPrefix("testSourceWithFailure");
    List<String> strings = prefixedStrings(IntStream.range(0, 100), prefix);

    CollectSink sink = new CollectSink(prefix);
    Publisher<MessageMetadata> publisher = getPublisher();

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(2)
            .enableCheckpointing(100);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.MILLISECONDS)));
    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .filter(
            v -> {
              if (v.equals(strings.get(37)) && staticSet.add("mapFailOnce")) {
                throw new RuntimeException("oh no, it's 37!");
              }
              return true;
            })
        .addSink(sink);
    JobClient client = env.executeAsync();

    ApiFutures.allAsList(
            strings.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    while (sink.values().size() < 100) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(sink.values()).containsExactlyElementsIn(strings);
    client.cancel().get();
  }

  @Test
  public void testSink() throws Exception {
    final String prefix = computeTestPrefix("testSink");
    List<String> strings = prefixedStrings(IntStream.range(0, 100), prefix);

    CollectSink sink = new CollectSink(prefix);

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(2)
            .enableCheckpointing(100);
    env.fromCollection(strings)
        .addSink(new PubsubLiteSink<>(sinkSettings().build()))
        .name("PSL Sink");

    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(sink);

    JobClient client = env.executeAsync();

    while (sink.values().size() < 100) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(sink.values()).containsExactlyElementsIn(strings);
    client.cancel().get();
  }

  @Test
  public void testSinkWithFailure() throws Exception {
    final String prefix = computeTestPrefix("testSinkWithFailure");
    List<String> strings = prefixedStrings(IntStream.range(0, 100), prefix);

    CollectSink sink = new CollectSink(prefix);

    // Set up a publisher which will fail once when attempting to publish the 37th message
    Publisher<MessageMetadata> publisher =
        spy(PerServerPublisherCache.getOrCreate(sinkSettings().build().getPublisherConfig()));
    Mockito.doAnswer(
            inv -> {
              Message m = inv.getArgument(0);
              if (m.data().toStringUtf8().equals(strings.get(37))
                  && staticSet.add("publishFailOnce")) {
                return ApiFutures.immediateFailedFuture(new RuntimeException("failure"));
              }
              return inv.callRealMethod();
            })
        .when(publisher)
        .publish(any());
    PerServerPublisherCache.getCache().set(sinkSettings().build().getPublisherConfig(), publisher);

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(2)
            .enableCheckpointing(100);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.MILLISECONDS)));

    env.fromCollection(strings)
        .addSink(new PubsubLiteSink<>(sinkSettings().build()))
        .name("PSL Sink");

    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(sink);

    JobClient client = env.executeAsync();

    while (sink.values().size() < 100) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(sink.values()).containsExactlyElementsIn(strings);
    client.cancel().get();
  }

  // A static set of strings for use in test
  private static final Set<String> staticSet = Collections.synchronizedSet(new HashSet<>());

  // A testing sink which stores messages in a static map to prevent them from being lost when
  // the sink is serialized.
  private static class CollectSink implements SinkFunction<String>, Serializable {
    // Note: doesn't store duplicates.
    private static final Multimap<String, String> collector =
        com.google.common.collect.Multimaps.synchronizedMultimap(HashMultimap.create());
    private final String key;

    CollectSink(String key) {
      this.key = key;
    }

    @Override
    public void invoke(String value) throws Exception {
      if (value.startsWith(key)) {
        collector.put(key, value);
      }
    }

    public Collection<String> values() {
      return collector.get(key);
    }
  }
}
