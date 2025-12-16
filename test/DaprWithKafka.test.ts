import path from "node:path";
import { describe, it } from "node:test"
import { promisify } from "node:util";
import { expect } from "expect";
import bodyParser from "body-parser";
import express from "express";
import { Network, TestContainers } from "testcontainers";
import { DaprClient, LogLevel } from "@dapr/dapr";
import { DaprContainer } from "@dapr/testcontainer-node";
import { KafkaContainer } from "@testcontainers/kafka";
import { assertMessageProducedAndConsumed } from "./kafka-test-helper.ts";

const DAPR_RUNTIME_IMAGE = "daprio/daprd:1.16.4";
const KAFKA_IMAGE = "confluentinc/cp-kafka:8.1.0";
const __dirname = import.meta.dirname;

describe("DaprWithKafka", { timeout: 240_000 }, () => {

  it("should connect with custom network", async () => {
    await using network = await new Network().start();
    await using container = await new KafkaContainer(KAFKA_IMAGE).withNetwork(network).start();

    await assertMessageProducedAndConsumed(container);
  });

  it("should provide pubsub via Kafka", { timeout: 60_000 }, async () => {
    const app = express();
    app.use(bodyParser.json({ type: "application/*+json" }));

    // Promise to resolve when the data is received
    let receiver: (data?: unknown) => void;
    const promise = new Promise((res) => {
      receiver = res;
    });

    app.post("/events", (req, res) => {
      const data = req.body.data;
      console.log("Received data:", data);
      res.sendStatus(200);
      receiver(data);
    });

    const appPort = 8081;
    const server = app.listen(appPort, () => {
      console.log(`Server is listening on port ${appPort}`);
    });
    await TestContainers.exposeHostPorts(appPort);

    const network = await new Network().start();
    const dapr = new DaprContainer(DAPR_RUNTIME_IMAGE)
      .withNetwork(network)
      .withAppPort(appPort)
      .withDaprLogLevel("info")
      .withDaprApiLoggingEnabled(false)
      .withAppChannelAddress("host.testcontainers.internal");
    const startedContainer = await dapr.start();

    const client = new DaprClient({
      daprHost: startedContainer.getHost(),
      daprPort: startedContainer.getHttpPort().toString(),
      logger: { level: LogLevel.Debug },
    });

    console.log("Publishing message...");
    await client.pubsub.publish("pubsub", "topic", { key: "key", value: "value" });

    console.log("Waiting for data...");
    const data = await promise;
    expect(data).toEqual({ key: "key", value: "value" });

    await client.stop();
    await startedContainer.stop();
    await network.stop();
    await promisify(server.close.bind(server))();
  });

  it("should route messages programmatically", { timeout: 60_000 }, async () => {
    const app = express();
    app.use(bodyParser.json({ type: "application/*+json" }));

    // Promise to resolve when the data is received
    let receiver: (data?: unknown) => void;
    const promise = new Promise((res) => {
      receiver = res;
    });

    app.get("/dapr/subscribe", (req, res) => {
      res.json([
        {
          pubsubname: "pubsub",
          topic: "orders",
          routes: {
            default: "/orders",
          },
        },
      ]);
    });

    app.post("/orders", (req, res) => {
      const data = req.body.data;
      console.log("Received data:", data);
      res.sendStatus(200);
      receiver(data);
    });

    const appPort = 8082;
    const server = app.listen(appPort, () => {
      console.log(`Server is listening on port ${appPort}`);
    });
    await TestContainers.exposeHostPorts(appPort);

    const network = await new Network().start();
    const dapr = new DaprContainer(DAPR_RUNTIME_IMAGE)
      .withNetwork(network)
      .withAppPort(appPort)
      .withDaprLogLevel("info")
      .withDaprApiLoggingEnabled(false)
      .withAppChannelAddress("host.testcontainers.internal");
    const startedContainer = await dapr.start();

    const client = new DaprClient({
      daprHost: startedContainer.getHost(),
      daprPort: startedContainer.getHttpPort().toString(),
      logger: { level: LogLevel.Debug },
    });

    console.log("Publishing message...");
    await client.pubsub.publish("pubsub", "orders", { key: "key", value: "value" });

    console.log("Waiting for data...");
    const data = await promise;
    expect(data).toEqual({ key: "key", value: "value" });

    await client.stop();
    await startedContainer.stop();
    await network.stop();
    await promisify(server.close.bind(server))();
  });
});
