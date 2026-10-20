## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia

Konfiguracja Dockera bez zmian, testy uruchamia ta sama komenda:

```bash
docker compose --profile test run --build --rm tests
```

- `--build` jest potrzebne przy kolejnych uruchomieniach po zmianach w kodzie:
  obraz `tests` kopiuje kod przy budowaniu, więc bez tej flagi biegnie stara
  wersja testów. Przy pierwszym uruchomieniu obraz i tak jest budowany.
- Przed testami `beforeAll` wysyła zamówienie testowe i czeka na nie na
  `orders-processed` (do 60 s). Zaraz po (re)starcie `consumer`a trwa to
  ok. 20–25 s — powód w „Znanych ograniczeniach”. Cały zestaw: ~35–40 s.

### Zmiany w `kafkaHelpers.ts`

**Deadlock w `waitForMessage` i `collectMessages`.** Po znalezieniu wiadomości
helper wywoływał `await consumer.disconnect()` z wnętrza `eachMessage`, a
`resolve` dopiero po nim. `disconnect()` w kafkajs czeka na zakończenie
bieżącego `eachMessage`, który czekał na `disconnect()` — Promise nigdy się
nie rozwiązywał, każdy test kończył się na limicie Jest (cały zestaw ~8 min
zamiast ~40 s), a consumer testowy zostawał podłączony. Teraz `eachMessage`
tylko rozwiązuje Promise, a `disconnect()` jest w `finally`, po wyjściu z
handlera.

### Podejście do testowania

Testy traktują system jak czarną skrzynkę: wysyłają wiadomość na `orders` i
sprawdzają wynik na `orders-processed` / `orders-dlq` — zawsze cały obiekt
(dokładny komunikat błędu, `originalMessage` / `raw`), nie tylko to, że „coś
przyszło”. Izolacja opiera się na unikalnych UUID w każdym teście: helpery
czytają topic od początku nową consumer group, więc testy szukają tylko swoich
wiadomości i nie przeszkadzają im dane z poprzednich uruchomień ani kolejność
testów. Scenariusze negatywne sprawdzają też drugą stronę — że poprawne
zamówienie nie trafia na DLQ, a niepoprawne nie trafia na `orders-processed`.
Synchronizacja wyłącznie przez polling z timeoutem, bez `sleep`.

### Znane ograniczenia / co zrobiłbym inaczej mając więcej czasu

- **Asercja „wiadomość nie przyszła” to okno 5 s.** Sprawdzamy ją dopiero po
  dotarciu wyniku na właściwy topic, więc nie da fałszywie czerwonego testu,
  ale przy dużych topikach helper może nie zdążyć przeczytać wszystkiego i
  test przejdzie fałszywie. Lepiej: czytać do końcowych offsetów partycji
  (`admin.fetchTopicOffsets`) zamiast czekać określony czas.
- **Wolny start po restarcie `consumer`a (~20–25 s).** `consumer.ts` nie
  obsługuje `SIGTERM`, więc zatrzymana instancja nie opuszcza consumer group;
  rebalance czeka na jej session timeout i przez ten czas partycje `orders`
  nie mają właściciela. Testy są na to odporne (readiness check), ale
  przyczynę warto naprawić w `consumer` (graceful shutdown) — na produkcji
  oznacza to przestój przy każdym deployu.
- **Koszt helperów rośnie z danymi.** Każde wywołanie tworzy nową consumer
  group i czyta topic od offsetu 0; w Kafce zostają osierocone grupy `test-…`.
- **Typowanie helperów.** Zwracają `any` (typy wiadomości DLQ zdefiniowane po
  stronie testów) i nie zwracają klucza, więc testy nie sprawdzają, że key
  wyniku == `orderId`. `collectMessages` nie wykrywa duplikatów — „dokładnie
  raz” nie jest sprawdzane.
- **Payload, który jest poprawnym JSON-em, ale nie obiektem** (`null`, `123`,
  `"abc"`): z analizy kodu `validate()` rzuca `TypeError` poza `try/catch`,
  więc wiadomość nie trafi na DLQ, tylko będzie ponawiana w kółko i zablokuje
  partycję (poison message). Do zweryfikowania i naprawy.
- **Luźna walidacja w `consumer`:** typ `customer` nie jest sprawdzany,
  `amount` może być ujemne, wejściowy `status` jest ignorowany i nadpisywany
  (testy dokumentują to ostatnie jako obecne zachowanie). Pusta wiadomość jest
  pomijana po cichu, bez DLQ.
- **Brak wspólnego kontraktu wiadomości** — typ zamówienia jest zdefiniowany
  osobno w `producer`, `consumer` i helperach; zmiana formatu w `consumer`
  nie zepsuje kompilacji testów (rozwiązanie: wspólny pakiet typów / schema
  registry).
- Uruchomienie testów z hosta (`localhost:9092`) nie jest zweryfikowane: broker
  ogłasza adres `kafka:9092`, którego host nie rozwiąże bez dodatkowego
  listenera.
