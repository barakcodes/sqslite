import { create, builder, ZodValidator } from "./queue";
import { Database } from "bun:sqlite";
import { z } from "zod";

const eventBuilder = builder({
  validator: ZodValidator,
});

const hello = eventBuilder(
  "app.hello",
  z.object({
    foo: z.string(),
  }),
);
const db = new Database("queue.db", { strict: true });
const newQueue = create({
  db: db,
  name: "notifications",
  options: {
    maxRetries: 3,
    retryDelay: 10_000,
  },
});

newQueue.publish(hello, {
  foo: "bar",
});

newQueue.handle([hello], async (message) => {
  console.log("message", message.properties);
  throw new Error("failed");
});
