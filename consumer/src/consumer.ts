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
 * Fix (O2): `JSON.parse` zwraca dowolną wartość JSON, a wynik był przypisywany
 * do `Record<string, unknown>` bez sprawdzenia. Dla payloadu `null`, `123` czy
 * `"abc"` operator `in` w `validate()` rzucał `TypeError` w `eachMessage` —
 * kafkajs nie restartuje `consumer`a po takim błędzie, więc jedna wiadomość
 * zatrzymywała przetwarzanie wszystkich partycji. Teraz taka wiadomość trafia
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
 * Loguje crash `consumer`a i kończy proces, gdy kafkajs nie planuje restartu.
 *
 * Fix: crash (np. wyjątek w `eachMessage`) był niewidoczny — `logLevel.NOTHING`
 * i brak listenera. W reprodukcji O2 `consumer` wychodził z grupy
 * (`LeaveGroup`), kafkajs restartował go dopiero po ~6 min, a przez ten czas
 * nie były przetwarzane żadne partycje; kontener był `Up`, logi puste. Teraz
 * każdy crash jest logowany. Gdy kafkajs nie zrestartuje `consumer`a
 * (`restart: false`), proces nadal by działał (połączenie producenta trzyma
 * event loop) — dlatego kończymy go kodem 1, a Docker
 * (`restart: unless-stopped`) uruchamia `consumer`a ponownie.
 */
function exitOnFatalCrash(consumer: Consumer): void {
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    log(`Crash konsumenta (restart: ${payload.restart}): ${payload.error.stack ?? payload.error.message}`);
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
