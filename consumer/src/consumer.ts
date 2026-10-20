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
 * Fix: `JSON.parse` zwraca dowolną wartość JSON, a wynik był przypisywany
 * do `Record<string, unknown>` bez sprawdzenia. Dla payloadu `null`, `123` czy
 * `"abc"` operator `in` w `validate()` rzucał `TypeError` w `eachMessage` —
 * kafkajs nie restartuje `consumer`a po takim błędzie (patrz
 * `exitOnFatalCrash`), więc jedna wiadomość zatrzymywała przetwarzanie
 * wszystkich partycji. Teraz taka wiadomość trafia
 * na DLQ z błędem `NOT_AN_OBJECT_ERROR`; tablice traktujemy tak samo (wcześniej
 * dawały mylące „Brakujące pola”).
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
 * Pierwotny błąd. kafkajs opakowuje wyjątek z `eachMessage` w swój błąd
 * (`cause`), którego stack wskazuje tylko na `kafkajs/src/retry`, a nie na
 * miejsce w naszym kodzie.
 */
function rootCause(error: Error): Error {
  return "cause" in error && error.cause instanceof Error ? rootCause(error.cause) : error;
}

/**
 * Loguje crash `consumer`a i kończy proces, gdy kafkajs nie planuje restartu.
 *
 * Fix: wyjątek z `eachMessage` typu `TypeError`, `RangeError`,
 * `ReferenceError` lub `SyntaxError` kafkajs uznaje za nienaprawialny: nie
 * ponawia przetwarzania, rozłącza `consumer`a (`LeaveGroup`) i nie restartuje
 * go (`restart: false`). Proces działał dalej (połączenie producenta trzyma
 * event loop), więc kontener był `Up`, logi puste (`logLevel.NOTHING`, brak
 * listenera), a żadna partycja nie była przetwarzana — tak kończył się
 * payload `null` przed `isJsonObject`.
 * Teraz każdy crash jest logowany, a przy `restart: false` proces kończy się
 * kodem 1, więc Docker (`restart: unless-stopped`) uruchamia `consumer`a
 * ponownie.
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
