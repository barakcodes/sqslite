import { ZodSchema, z } from "zod";
import { sqslite, type SqliteDatabase } from "./index";

export const create = <Db extends SqliteDatabase>(params: {
  db: Db;
  name: string;
  options?: {
    cacheSize?: number;
    maxRetries?: number;
    retryDelay?: number;
  };
}) => {
  const name = params.name;
  const cacheSize = params.options?.cacheSize || 10_000;
  sqslite.setup(params.db, {
    cacheSize,
  });

  return {
    publish: async <Events extends Definition = any>(
      def: Events | string,
      properties: Events["$input"],
    ) => {
      const evt =
        typeof def === "string"
          ? {
              type: def,
              properties,
              metadata: {},
            }
          : await def.create(properties, {});
      sqslite.sendMessage(params.db, name, evt.type, evt.properties);
    },
    handle: async <Events extends Definition>(
      messages: Events | Events[],
      handler: (
        message: {
          [K in Events["type"]]: Extract<Events, { type: K }>["$payload"];
        }[Events["type"]],
      ) => Promise<void>,
    ) => {
      console.log("Listening for messages ...");
      for await (const message of sqslite.poller(params.db, {
        name,
        interval: 10_000,
        types: Array.isArray(messages)
          ? messages.map((m) => m.type)
          : [messages.type],
        opts: {
          maxRetries: params.options?.maxRetries || 3,
          timeout: params.options?.retryDelay || 10_000,
        },
      })) {
        try {
          const properties = JSON.parse(
            message.body.toString("utf-8"),
          ) as Events["$input"];
          const event = {
            properties,
            type: message.type as Events["type"],
          };
          await handler(event);
          sqslite.deleteMessage(params.db, name, message.id);
        } catch (e) {
          console.error(e);
          throw e;
        }
      }
    },
  };
};
// ripped off from the sst ion codebase
type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

type Definition = {
  type: string;
  $input: any;
  $output: any;
  $metadata: any;
  $payload: any;
  create: (...args: any[]) => Promise<any>;
};

export function builder<
  Metadata extends
    | ((type: string, properties: any) => any)
    | Parameters<Validator>[0],
  Validator extends (schema: any) => (input: any) => any,
>(input: { validator: Validator; metadata?: Metadata }) {
  const validator = input.validator;
  const fn = function event<
    Type extends string,
    Schema extends Parameters<Validator>[0],
  >(type: Type, schema: Schema) {
    type MetadataOutput = Metadata extends (
      type: string,
      properties: any,
    ) => any
      ? ReturnType<Metadata>
      : // @ts-expect-error
        inferParser<Metadata>["out"];
    type Payload = Prettify<{
      type: Type;
      properties: Parsed["out"];
      metadata: MetadataOutput;
    }>;
    type Parsed = inferParser<Schema>;
    type Create = Metadata extends (type: string, properties: any) => any
      ? (properties: Parsed["in"]) => Promise<Payload>
      : (
          properties: Parsed["in"],
          // @ts-expect-error
          metadata: inferParser<Metadata>["in"],
        ) => Promise<Payload>;
    const validate = validator(schema);
    async function create(properties: any, metadata?: any) {
      metadata = input.metadata
        ? typeof input.metadata === "function"
          ? input.metadata(type, properties)
          : input.metadata(metadata)
        : {};
      properties = validate(properties);
      return {
        type,
        properties,
        metadata,
      };
    }
    return {
      create: create as Create,
      type,
      $input: {} as Parsed["in"],
      $output: {} as Parsed["out"],
      $payload: {} as Payload,
      $metadata: {} as MetadataOutput,
    } satisfies Definition;
  };
  fn.coerce = <Events extends Definition>(
    _events: Events | Events[],
    raw: any,
  ): {
    [K in Events["type"]]: Extract<Events, { type: K }>["$payload"];
  }[Events["type"]] => {
    return raw;
  };
  return fn;
}

// Taken from tRPC
type ParserZodEsque<TInput, TParsedInput> = {
  _input: TInput;
  _output: TParsedInput;
};

type ParserMyZodEsque<TInput> = {
  parse: (input: any) => TInput;
};

type ParserWithoutInput<TInput> = ParserMyZodEsque<TInput>;

type ParserWithInputOutput<TInput, TParsedInput> = ParserZodEsque<
  TInput,
  TParsedInput
>;

type Parser = ParserWithInputOutput<any, any> | ParserWithoutInput<any>;

type inferParser<TParser extends Parser> =
  TParser extends ParserWithInputOutput<infer $TIn, infer $TOut>
    ? {
        in: $TIn;
        out: $TOut;
      }
    : TParser extends ParserWithoutInput<infer $InOut>
      ? {
          in: $InOut;
          out: $InOut;
        }
      : never;

export function ZodValidator<Schema extends ZodSchema>(
  schema: Schema,
): (input: z.input<Schema>) => z.output<Schema> {
  return (input) => {
    return schema.parse(input);
  };
}
