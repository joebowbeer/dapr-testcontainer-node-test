import { describe, it } from "node:test";
import { expect } from "expect";
import { Network } from "testcontainers";
import { KafkaContainer } from "@testcontainers/kafka";
import { assertMessageProducedAndConsumed } from "./kafka-test-helper.ts";

const IMAGE = "confluentinc/cp-kafka:8.1.0";

describe("KafkaContainer", { timeout: 240_000 }, () => {

  it("should connect", async () => {
    await using container = await new KafkaContainer(IMAGE).start();

    await assertMessageProducedAndConsumed(container);
  });

  it("should connect with custom network", async () => {
    await using network = await new Network().start();
    await using container = await new KafkaContainer(IMAGE).withNetwork(network).start();

    await assertMessageProducedAndConsumed(container);
  });

  it("should be reusable", async () => {
    await using container1 = await new KafkaContainer(IMAGE).withReuse().start();
    const container2 = await new KafkaContainer(IMAGE).withReuse().start();

    expect(container2.getId()).toBe(container1.getId());
  });
});
