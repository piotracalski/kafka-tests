/**
 * Testy E2E przepływu: orders -> consumer -> orders-processed / orders-dlq.
 *
 * Topiki zawierają dane z poprzednich uruchomień, więc każdy test generuje
 * własne UUID (`orderId` i/lub `customer`) i szuka tylko swoich wiadomości.
 *
 *  - Asercje obejmują cały obiekt (`toStrictEqual`), a nie tylko to, że coś
 *    przyszło.
 *  - Błędy walidacji odnajdujemy na DLQ po całym `originalMessage`. Niepoprawne
 *    zamówienia mają unikalne `orderId` i `customer`, więc da się je znaleźć
 *    nawet bez `orderId`.
 *  - Kafka gwarantuje kolejność tylko w obrębie partycji, więc wiele zamówień
 *    porównujemy jako zbiór.
 */
import { randomUUID } from "crypto";
import { isDeepStrictEqual } from "util";
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

/** Wiadomość na DLQ, gdy wejście nie jest poprawnym JSON-em. */
interface InvalidJsonDlqMessage {
  error: string;
  raw: string;
}

/** Wiadomość na DLQ, gdy JSON jest poprawny, ale nie przechodzi walidacji. */
interface ValidationDlqMessage {
  error: string;
  originalMessage: Record<string, unknown>;
}

/** Wiadomość na DLQ, gdy poprawny JSON nie jest obiektem (`null`, liczba, string, tablica). */
interface NotAnObjectDlqMessage {
  error: "Wiadomość musi być obiektem JSON.";
  originalMessage: unknown;
}

/**
 * Okno asercji „wiadomość nie przyszła”. Sprawdzamy ją dopiero po dotarciu
 * wyniku na właściwy topic (`consumer` publikuje jeden wynik na wiadomość),
 * więc okno musi pokryć tylko odczyt drugiego topicu.
 */
const ABSENCE_WINDOW_SECONDS = 5;
/**
 * Testy z asercją negatywną czekają dwa razy: na wynik (do 20 s) i na brak
 * wiadomości (5 s), a każdy helper dolicza connect i join grupy. Przy
 * domyślnym limicie Jest (30 s) wolne, ale poprawne przetwarzanie mogłoby
 * przerwać test, zanim asercja zdąży zadziałać.
 */
const TWO_WAITS_TEST_TIMEOUT_MS = 45_000;
/** Patrz waitUntilFlowIsReady. */
const FLOW_READY_TIMEOUT_SECONDS = 60;

const hasOrderId = (orderId: string) => (value: Partial<Order>) => value.orderId === orderId;

const isDlqOf = (payload: unknown) => (value: { originalMessage?: unknown }) =>
  isDeepStrictEqual(value.originalMessage, payload);

/** Poprawne zamówienie z unikalnym też `customer`, żeby dało się je znaleźć bez `orderId`. */
function makeUniqueOrder(): Order {
  return makeTestOrder({ customer: `Klient ${randomUUID()}` });
}

/**
 * Readiness check: wysyła zamówienie testowe i czeka na nie na orders-processed.
 * Po restarcie `consumer` przez pewien czas nie ma przypisanych partycji
 * `orders`: stara instancja nie opuszcza grupy (brak graceful shutdown), więc
 * rebalance czeka na jej session timeout (~30 s). Bez tego pierwszy test po
 * `docker compose run` dostawał `null` po 20 s, choć zamówienie było
 * przetwarzane chwilę później.
 */
async function waitUntilFlowIsReady(producer: Producer): Promise<void> {
  const probe = makeTestOrder();
  await sendOrder(producer, probe);

  const processed = await waitForMessage(
    ORDERS_PROCESSED_TOPIC,
    hasOrderId(probe.orderId),
    FLOW_READY_TIMEOUT_SECONDS
  );
  if (processed === null) {
    throw new Error(
      `\`consumer\` nie przetworzył zamówienia testowego w ${FLOW_READY_TIMEOUT_SECONDS} s — ` +
        "czy działa? (docker compose ps, docker compose logs consumer)"
    );
  }
}

function withoutField(order: Order, field: keyof Order): Partial<Order> {
  const copy: Partial<Order> = { ...order };
  delete copy[field];
  return copy;
}

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();
    await waitUntilFlowIsReady(producer);
  }, (FLOW_READY_TIMEOUT_SECONDS + 10) * 1000);

  afterAll(async () => {
    await disconnectProducer();
  });

  /** Wysyła dowolny obiekt jako JSON, także z polami o typach niezgodnych z `Order`. */
  async function sendPayload(payload: Record<string, unknown>): Promise<void> {
    await sendRaw(producer, JSON.stringify(payload));
  }

  async function waitForProcessed(orderId: string): Promise<Order | null> {
    return waitForMessage(ORDERS_PROCESSED_TOPIC, hasOrderId(orderId));
  }

  async function waitForValidationDlq(payload: object): Promise<ValidationDlqMessage | null> {
    return waitForMessage(ORDERS_DLQ_TOPIC, isDlqOf(payload));
  }

  describe("poprawne zamówienie", () => {
    it("trafia na orders-processed bez zmian, ze statusem PROCESSED", async () => {
      const order = makeTestOrder();

      await sendOrder(producer, order);

      expect(await waitForProcessed(order.orderId)).toStrictEqual({
        ...order,
        status: "PROCESSED",
      });
    });

    it(
      "nie trafia na orders-dlq",
      async () => {
        const order = makeTestOrder();

        await sendOrder(producer, order);
        expect(await waitForProcessed(order.orderId)).not.toBeNull();

        const dlqMessage = await waitForMessage(
          ORDERS_DLQ_TOPIC,
          (value: Partial<ValidationDlqMessage>) => value.originalMessage?.orderId === order.orderId,
          ABSENCE_WINDOW_SECONDS
        );
        expect(dlqMessage).toBeNull();
      },
      TWO_WAITS_TEST_TIMEOUT_MS
    );

    it("traci pola spoza kontraktu", async () => {
      const order = makeTestOrder();
      const orderWithExtras = { ...order, coupon: "RABAT10", notes: "pilne" };

      await sendOrder(producer, orderWithExtras);

      expect(await waitForProcessed(order.orderId)).toStrictEqual({
        ...order,
        status: "PROCESSED",
      });
    });

    it("dostaje status PROCESSED niezależnie od statusu wejściowego", async () => {
      const order = makeTestOrder({ status: "CANCELLED" });

      await sendOrder(producer, order);

      expect(await waitForProcessed(order.orderId)).toStrictEqual({
        ...order,
        status: "PROCESSED",
      });
    });
  });

  describe("wiele zamówień naraz", () => {
    it("każde trafia na orders-processed (kolejność między partycjami nie jest gwarantowana)", async () => {
      const orders = Array.from({ length: 5 }, () => makeTestOrder());
      const pendingOrderIds = new Set(orders.map((order) => order.orderId));

      await Promise.all(orders.map((order) => sendOrder(producer, order)));

      const processed: Order[] = await collectMessages(
        ORDERS_PROCESSED_TOPIC,
        (value: Partial<Order>) => value.orderId !== undefined && pendingOrderIds.delete(value.orderId),
        orders.length
      );

      expect(processed).toHaveLength(orders.length);
      expect(processed).toEqual(
        expect.arrayContaining(orders.map((order) => ({ ...order, status: "PROCESSED" })))
      );
    });
  });

  describe("niepoprawny JSON", () => {
    it(
      "trafia na orders-dlq z polami error i raw, a nie na orders-processed",
      async () => {
        const orderId = randomUUID();
        const raw = `{"orderId": "${orderId}", "customer": `;

        await sendRaw(producer, raw, orderId);

        const dlqMessage: InvalidJsonDlqMessage | null = await waitForMessage(
          ORDERS_DLQ_TOPIC,
          (value: Partial<InvalidJsonDlqMessage>) => value.raw === raw
        );
        expect(dlqMessage).toStrictEqual({
          error: expect.stringMatching(/^Niepoprawny JSON: /),
          raw,
        });

        expect(await waitForMessage(ORDERS_PROCESSED_TOPIC, hasOrderId(orderId), ABSENCE_WINDOW_SECONDS)).toBeNull();
      },
      TWO_WAITS_TEST_TIMEOUT_MS
    );
  });

  describe("JSON, który nie jest obiektem", () => {
    it.each([
      { type: "string", makePayload: (marker: string): unknown => marker },
      { type: "tablica", makePayload: (marker: string): unknown => [marker] },
    ])("$type trafia na orders-dlq z komunikatem, że wiadomość musi być obiektem", async ({ makePayload }) => {
      const payload = makePayload(randomUUID());

      await sendRaw(producer, JSON.stringify(payload));

      const dlqMessage: NotAnObjectDlqMessage | null = await waitForMessage(ORDERS_DLQ_TOPIC, isDlqOf(payload));
      expect(dlqMessage).toStrictEqual({ error: "Wiadomość musi być obiektem JSON.", originalMessage: payload });
    });

    /**
     * Test regresji: przed fixem prymitywy (`null`, liczba, boolean, string)
     * powodowały crash `consumer`a i zatrzymywały wszystkie partycje, a tablica
     * trafiała na DLQ jako „Brakujące pola”. Ten sam key oznacza tę samą
     * partycję, więc zamówienie jest przetwarzane dopiero po złej wiadomości.
     */
    it.each(["null", "123", "true", '"abc"', "[1, 2]"])(
      "payload %s nie blokuje kolejnego zamówienia z tym samym kluczem",
      async (raw) => {
        const order = makeTestOrder();

        await sendRaw(producer, raw, order.orderId);
        await sendOrder(producer, order);

        expect(await waitForProcessed(order.orderId)).toStrictEqual({ ...order, status: "PROCESSED" });
      }
    );
  });

  describe("walidacja zamówienia", () => {
    it.each<keyof Order>(["orderId", "customer", "amount", "status"])(
      "zamówienie bez pola %s trafia na orders-dlq z listą brakujących pól",
      async (field) => {
        const payload = withoutField(makeUniqueOrder(), field);

        await sendOrder(producer, payload);

        expect(await waitForValidationDlq(payload)).toStrictEqual({
          error: `Brakujące pola: [${field}]`,
          originalMessage: payload,
        });
      }
    );

    it.each([
      { field: "amount", value: "123.45", type: "string", error: "Pole 'amount' musi być liczbą." },
      { field: "orderId", value: 123, type: "number", error: "Pole 'orderId' musi być stringiem." },
    ])("zamówienie z polem $field typu $type trafia na orders-dlq z komunikatem: $error", async ({ field, value, error }) => {
      const payload = { ...makeUniqueOrder(), [field]: value };

      await sendPayload(payload);

      expect(await waitForValidationDlq(payload)).toStrictEqual({ error, originalMessage: payload });
    });

    it("zamówienie z kilkoma błędami naraz trafia na orders-dlq ze wszystkimi komunikatami rozdzielonymi '; '", async () => {
      const payload = { orderId: randomUUID(), amount: "dużo" };

      await sendPayload(payload);

      expect(await waitForValidationDlq(payload)).toStrictEqual({
        error: "Brakujące pola: [customer, status]; Pole 'amount' musi być liczbą.",
        originalMessage: payload,
      });
    });

    it(
      "niepoprawne zamówienie nie trafia na orders-processed",
      async () => {
        const order = makeUniqueOrder();
        const payload = withoutField(order, "status");

        await sendOrder(producer, payload);
        expect(await waitForValidationDlq(payload)).not.toBeNull();

        expect(
          await waitForMessage(ORDERS_PROCESSED_TOPIC, hasOrderId(order.orderId), ABSENCE_WINDOW_SECONDS)
        ).toBeNull();
      },
      TWO_WAITS_TEST_TIMEOUT_MS
    );
  });
});
