import path from "node:path";
import { describe, it } from "node:test"
import { expect } from "expect";
import bodyParser from "body-parser";
import express from "express";
import { Network, TestContainers } from "testcontainers";
import { DaprClient, LogLevel } from "@dapr/dapr";
import { DaprContainer, Subscription } from "@dapr/testcontainer-node";
import { KafkaContainer } from "@testcontainers/kafka";

const DAPR_IMAGE = "daprio/daprd:1.16.4";
const KAFKA_IMAGE = "confluentinc/cp-kafka:8.1.0";
const __dirname = import.meta.dirname;

describe("DaprWithKafka", { timeout: 240_000 }, () => {

  it("should do Kafka pubsub", { timeout: 60_000 }, async () => {
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
    await using _server = app.listen(appPort, () => {
      console.log(`Server is listening on port ${appPort}`);
    });
    await TestContainers.exposeHostPorts(appPort);

    await using network = await new Network().start();

    await using _kafka = await new KafkaContainer(KAFKA_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .start();

    const componentPath = path.join(__dirname, "__fixtures__", "dapr-resources", "pubsub.yaml");

    const dapr = new DaprContainer(DAPR_IMAGE)
      .withNetwork(network)
      .withAppPort(appPort)
      .withDaprLogLevel("info")
      .withDaprApiLoggingEnabled(true)
      .withComponentFromPath(componentPath)
      .withSubscription(new Subscription("my-subscription", "kafka-pubsub-noauth", "topic", undefined, "/events"))
      .withAppChannelAddress("host.testcontainers.internal");

    const components = dapr.getComponents();
    expect(components.length).toBe(1);
    const pubsub = components[0];
    // console.log(pubsub.toYaml());
    expect(pubsub.name).toBe("kafka-pubsub-noauth");

    const subscriptions = dapr.getSubscriptions();
    expect(subscriptions.length).toBe(1);
    const subscription = subscriptions[0];
    // console.log(subscription.toYaml());
    expect(subscription.pubsubName).toBe("kafka-pubsub-noauth");
    expect(subscription.topic).toBe("topic");

    // TODO: implement StartedDaprContainer.[Symbol.asyncDispose]
    const startedContainer = await dapr.start();

    // TODO: implement DaprClient.[Symbol.asyncDispose]
    const client = new DaprClient({
      daprHost: startedContainer.getHost(),
      daprPort: startedContainer.getHttpPort().toString(),
      logger: { level: LogLevel.Debug },
    });

    console.log("Publishing message...");
    await client.pubsub.publish("kafka-pubsub-noauth", "topic", { key: "key", value: "value" });

    console.log("Waiting for data...");
    const data = await promise;
    expect(data).toEqual({ key: "key", value: "value" });

    await client.stop();
    await startedContainer.stop();
  });

  it("should route Kafka messages programmatically", { timeout: 60_000 }, async () => {
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
          pubsubname: "kafka-pubsub-noauth",
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
    await using _server = app.listen(appPort, () => {
      console.log(`Server is listening on port ${appPort}`);
    });
    await TestContainers.exposeHostPorts(appPort);

    await using network = await new Network().start();

    await using _kafka = await new KafkaContainer(KAFKA_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .start();

    const componentPath = path.join(__dirname, "__fixtures__", "dapr-resources", "pubsub.yaml");

    const dapr = new DaprContainer(DAPR_IMAGE)
      .withNetwork(network)
      .withAppPort(appPort)
      .withDaprLogLevel("info")
      .withDaprApiLoggingEnabled(true)
      .withComponentFromPath(componentPath)
      .withAppChannelAddress("host.testcontainers.internal");

    const components = dapr.getComponents();
    expect(components.length).toBe(1);
    const pubsub = components[0];
    // console.log(pubsub.toYaml());
    expect(pubsub.name).toBe("kafka-pubsub-noauth");

    // TODO: implement StartedDaprContainer.[Symbol.asyncDispose]
    const startedContainer = await dapr.start();

    // TODO: implement DaprClient.[Symbol.asyncDispose]
    const client = new DaprClient({
      daprHost: startedContainer.getHost(),
      daprPort: startedContainer.getHttpPort().toString(),
      logger: { level: LogLevel.Debug },
    });

    console.log("Publishing message...");
    await client.pubsub.publish("kafka-pubsub-noauth", "orders", { key: "key", value: "value" });

    console.log("Waiting for data...");
    const data = await promise;
    expect(data).toEqual({ key: "key", value: "value" });

    await client.stop();
    await startedContainer.stop();
  });
});
