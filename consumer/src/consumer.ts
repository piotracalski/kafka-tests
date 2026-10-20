/**
 * Prosty konsument Kafka.
 *
 * Czyta wiadomości z topicu `orders`, waliduje ich strukturę i:
 *   - jeśli wiadomość jest poprawna -> zmienia status na PROCESSED
 *     i publikuje wynik na `orders-processed`,
 *   - jeśli wiadomość jest niepoprawna (brak wymaganych pól / zły JSON)
 *     -> publikuje ją (wraz z opisem błędu) na `orders-dlq` (dead-letter queue).
 *
 * Konsument działa w sposób ciągły (jak długo żyjący serwis).
 */
import { Kafka, Consumer, Producer, logLevel, EachMessagePayload } from "kafkajs";

const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092";
const INPUT_TOPIC = process.env.ORDERS_TOPIC || "orders";
const OUTPUT_TOPIC = process.env.ORDERS_PROCESSED_TOPIC || "orders-processed";
const DLQ_TOPIC = process.env.ORDERS_DLQ_TOPIC || "orders-dlq";
const GROUP_ID = process.env.CONSUMER_GROUP_ID || "orders-processing-service";

const REQUIRED_FIELDS = ["orderId", "customer", "amount", "status"] as const;

interface ProcessedOrder {
  orderId: string;
  customer: string;
  amount: number;
  status: "PROCESSED";
}

function log(message: string): void {
  console.log(`${new Date().toISOString()} [consumer] ${message}`);
}

const NOT_AN_OBJECT_ERROR = "Wiadomość musi być obiektem JSON.";

/**
 * Czy sparsowany JSON jest obiektem (a nie `null`, prymitywem ani tablicą).
 *
 * Fix: wynik `JSON.parse` trafiał do `validate()` bez sprawdzenia typu. Dla
 * payloadu `null`, `123` czy `"abc"` operator `in` rzucał `TypeError`, po którym
 * kafkajs nie restartuje `consumer`a (patrz `exitOnFatalCrash`), więc jedna
 * wiadomość zatrzymywała wszystkie partycje. Teraz taka wiadomość trafia na DLQ
 * z błędem `NOT_AN_OBJECT_ERROR`. Tablica też, bo wcześniej dawała mylące
 * „Brakujące pola”.
 */
function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Zwraca listę błędów walidacji (pusta tablica = wiadomość poprawna). */
function validate(order: Record<string, unknown>): string[] {
  const errors: string[] = [];

  const missing = REQUIRED_FIELDS.filter((field) => !(field in order));
  if (missing.length > 0) {
    errors.push(`Brakujące pola: [${missing.join(", ")}]`);
  }
  if ("amount" in order && typeof order.amount !== "number") {
    errors.push("Pole 'amount' musi być liczbą.");
  }
  if ("orderId" in order && typeof order.orderId !== "string") {
    errors.push("Pole 'orderId' musi być stringiem.");
  }

  return errors;
}

/**
 * Zwraca pierwotny błąd. kafkajs opakowuje wyjątek z `eachMessage` we własny
 * (`cause`), a stack wrappera wskazuje tylko na `kafkajs/src/retry`.
 */
function rootCause(error: Error): Error {
  return "cause" in error && error.cause instanceof Error ? rootCause(error.cause) : error;
}

/**
 * Loguje crash `consumer`a i kończy proces, gdy kafkajs nie planuje restartu.
 *
 * Fix: `TypeError`, `RangeError`, `ReferenceError` i `SyntaxError` z
 * `eachMessage` kafkajs uznaje za nienaprawialne. Nie ponawia przetwarzania,
 * rozłącza `consumer`a i go nie restartuje (`restart: false`). Proces działał
 * dalej (połączenie producenta trzyma event loop), kontener był `Up`, logi
 * puste, a żadna partycja nie była przetwarzana. Tak kończył się payload `null`
 * przed `isJsonObject`. Teraz każdy crash trafia do logu, a przy
 * `restart: false` proces kończy się kodem 1 i Docker uruchamia go ponownie.
 */
function exitOnFatalCrash(consumer: Consumer): void {
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    const error = rootCause(payload.error);
    log(`Crash konsumenta (restart: ${payload.restart}): ${error.stack ?? error.message}`);
    if (!payload.restart) {
      process.exit(1);
    }
  });
}

async function main(): Promise<void> {
  const kafka = new Kafka({
    clientId: "orders-consumer",
    brokers: [BOOTSTRAP_SERVERS],
    logLevel: logLevel.NOTHING,
    retry: {
      initialRetryTime: 2000,
      retries: 30,
    },
  });

  const consumer: Consumer = kafka.consumer({ groupId: GROUP_ID });
  const producer: Producer = kafka.producer();
  exitOnFatalCrash(consumer);

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: true });

  log(
    `Konsument uruchomiony. Nasłuchuję na '${INPUT_TOPIC}', zapisuję do '${OUTPUT_TOPIC}' (błędy: '${DLQ_TOPIC}').`
  );

  await consumer.run({
    eachMessage: async ({ message }: EachMessagePayload) => {
      const key = message.key ? message.key.toString("utf-8") : undefined;
      const rawValue = message.value;

      if (!rawValue) {
        return;
      }

      let order: unknown;
      try {
        order = JSON.parse(rawValue.toString("utf-8"));
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        log(`Niepoprawny JSON, wysyłam do DLQ. Błąd: ${errorMessage}`);
        await producer.send({
          topic: DLQ_TOPIC,
          messages: [
            {
              key,
              value: JSON.stringify({
                error: `Niepoprawny JSON: ${errorMessage}`,
                raw: rawValue.toString("utf-8"),
              }),
            },
          ],
        });
        return;
      }

      const errors = isJsonObject(order) ? validate(order) : [NOT_AN_OBJECT_ERROR];
      if (!isJsonObject(order) || errors.length > 0) {
        log(`Wiadomość niepoprawna, wysyłam do DLQ: ${JSON.stringify(order)} | błędy: ${errors.join("; ")}`);
        await producer.send({
          topic: DLQ_TOPIC,
          messages: [
            {
              key,
              value: JSON.stringify({
                error: errors.join("; "),
                originalMessage: order,
              }),
            },
          ],
        });
        return;
      }

      const processedOrder: ProcessedOrder = {
        orderId: order.orderId as string,
        customer: order.customer as string,
        amount: order.amount as number,
        status: "PROCESSED",
      };

      await producer.send({
        topic: OUTPUT_TOPIC,
        messages: [{ key, value: JSON.stringify(processedOrder) }],
      });
      log(`Przetworzono zamówienie ${processedOrder.orderId} -> ${OUTPUT_TOPIC}`);
    },
  });
}

main().catch((err) => {
  console.error("Błąd konsumenta:", err);
  process.exit(1);
});
