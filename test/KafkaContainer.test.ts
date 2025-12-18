import { describe, it } from "node:test";
import { expect } from "expect";
import { Network } from "testcontainers";
import { KafkaContainer } from "@testcontainers/kafka";
import { type GlobalConfig, KafkaJS } from "@confluentinc/kafka-javascript";
import { StartedKafkaContainer } from "@testcontainers/kafka";

const KAFKA_IMAGE = "confluentinc/cp-kafka:8.1.0";
const KAFKA_TLS_PORT = 9093;

describe("KafkaContainer", { timeout: 240_000 }, () => {

  it("should connect", async () => {
    await using container = await new KafkaContainer(KAFKA_IMAGE).start();

    await assertMessageProducedAndConsumed(container);
  });

  it("should connect with custom network", async () => {
    await using network = await new Network().start();
    await using container = await new KafkaContainer(KAFKA_IMAGE).withNetwork(network).start();

    await assertMessageProducedAndConsumed(container);
  });

  it("should be reusable", async () => {
    await using container1 = await new KafkaContainer(KAFKA_IMAGE).withReuse().start();
    const container2 = await new KafkaContainer(KAFKA_IMAGE).withReuse().start();

    expect(container2.getId()).toBe(container1.getId());
  });

  it("should create topic with admin interface", async () => {
    await using kafkaContainer = await new KafkaContainer(KAFKA_IMAGE)
      .withExposedPorts(KAFKA_TLS_PORT)
      .start();

    const port = kafkaContainer.getMappedPort(KAFKA_TLS_PORT);
    const host = kafkaContainer.getHost();

    const kafka = new KafkaJS.Kafka({
      kafkaJS: {
        brokers: [`${host}:${port}`],
        clientId: 'kafka_test_app',
        // logLevel: KafkaJS.logLevel.DEBUG,
      },
    });

    const topic = 'test-kafka-topic';
    const key = 'test-key'; // optional, depends on partitioning requirements
    const message = 'Hello World!';

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        {
          topic,
        },
      ],
    });
    await admin.disconnect();

    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic,
      messages: [
        {
          key: key, // optional, depends on partitioning requirements
          value: message,
        },
      ],
    });
    await producer.disconnect();

    const consumer = kafka.consumer({ kafkaJS: { groupId: "test-group", fromBeginning: true } });
    await consumer.connect();
    await consumer.subscribe({ topic });

    const consumedMessage = await new Promise((resolve) =>
      consumer.run({
        eachMessage: async ({ message }) => resolve(message.value?.toString()),
      })
    );
    await consumer.disconnect();

    expect(consumedMessage).toBe(message);
  });
});

async function assertMessageProducedAndConsumed(
  container: StartedKafkaContainer,
  additionalKafkaConfig: Partial<KafkaJS.KafkaConfig> = {},
  additionalGlobalConfig: Partial<GlobalConfig> = {}
) {
  const brokers = [`${container.getHost()}:${container.getMappedPort(KAFKA_TLS_PORT)}`];
  const kafka = new KafkaJS.Kafka({
    kafkaJS: {
      logLevel: KafkaJS.logLevel.ERROR,
      brokers,
      ...additionalKafkaConfig,
    },
    ...additionalGlobalConfig,
  });

  const producer = kafka.producer();
  await producer.connect();
  const consumer = kafka.consumer({ kafkaJS: { groupId: "test-group", fromBeginning: true } });
  await consumer.connect();

  await producer.send({ topic: "test-topic", messages: [{ value: "test message" }] });
  await consumer.subscribe({ topic: "test-topic" });

  const consumedMessage = await new Promise((resolve) =>
    consumer.run({
      eachMessage: async ({ message }) => resolve(message.value?.toString()),
    })
  );
  expect(consumedMessage).toBe("test message");

  await consumer.disconnect();
  await producer.disconnect();
}
