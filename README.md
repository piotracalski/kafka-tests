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
docker compose --profile test run --build --rm tests
```

Pojedynczy test (po fragmencie nazwy) i testy w losowej kolejności:

```bash
docker compose --profile test run --rm tests npm test -- -t "payload null"
docker compose --profile test run --rm tests npm test -- --randomize
```

Przy `--randomize` Jest wypisuje `Seed:`, a `--seed=<liczba>` odtwarza tę samą kolejność.

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera.
**Uwaga:** w obecnej konfiguracji to nie działa (`getaddrinfo ENOTFOUND kafka`),
szczegóły w „Znanych ograniczeniach”. Testy uruchamiaj w kontenerze.

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

- Bez `--build` po zmianie kodu biegnie stara wersja testów, bo obraz `tests`
  kopiuje kod przy budowaniu.
- `beforeAll` wysyła zamówienie testowe i czeka na nie na `orders-processed`
  (do 60 s). Zaraz po restarcie `consumer`a trwa to 20–25 s (patrz „Znane
  ograniczenia”). Cały zestaw trwa ok. 35–40 s z `--build` (odtwarza też
  kontener `consumer`) i ok. 16 s bez niego.

### Zmiany w `kafkaHelpers.ts`

**Deadlock w `waitForMessage` i `collectMessages`.** Po znalezieniu wiadomości
helper wywoływał `await consumer.disconnect()` wewnątrz `eachMessage`, a
`resolve` dopiero potem. `disconnect()` w kafkajs czeka, aż `eachMessage` się
zakończy, więc oba czekały na siebie nawzajem. Każdy test kończył się na
limicie Jest (cały zestaw ~8 min zamiast ~40 s), a consumer testowy zostawał
podłączony. Teraz `eachMessage` tylko rozwiązuje Promise, a `disconnect()` jest
w `finally`.

### Zmiany w `consumer`

**JSON, który nie jest obiektem** (`null`, `123`, `"abc"`). Wynik `JSON.parse`
trafiał do `validate()` bez sprawdzenia typu, a operator `in` na prymitywie
rzucał `TypeError`. kafkajs traktuje `TypeError` jako błąd nienaprawialny:
rozłącza `consumer`a i go nie restartuje. W reprodukcji jedna taka wiadomość
zatrzymała wszystkie partycje, a kontener był `Up` z pustymi logami. Teraz
taka wiadomość trafia na DLQ z błędem `"Wiadomość musi być obiektem JSON."`.
Tablica też, bo wcześniej dawała mylące „Brakujące pola”. Testy regresji
wysyłają taki payload z kluczem kolejnego zamówienia i sprawdzają, że
zamówienie zostaje przetworzone.

**Niewidoczny crash.** Przy `logLevel.NOTHING` i bez obsługi zdarzenia crash
nie zostawiał śladu w logach, a proces działał dalej, nic nie przetwarzając.
Listener `CRASH` loguje teraz każdy crash. Gdy kafkajs nie planuje restartu,
kończy proces kodem 1 i Docker (`restart: unless-stopped`) uruchamia
`consumer`a ponownie. Po fixie żadna znana wiadomość nie powoduje crashu, więc
nie ma na to testu E2E. Sprawdziłem to ręcznie: z wyłączonym `isJsonObject`
wiadomość `null` daje w logu `Crash konsumenta (restart: false)`, a Docker
restartuje kontener z rosnącym backoffem. To nadal pętla, ale widoczna w logach
i w liczniku restartów.

### Podejście do testowania

Testy traktują system jak czarną skrzynkę: wysyłają wiadomość na `orders` i
sprawdzają cały wynik na `orders-processed` albo `orders-dlq`, łącznie z
dokładnym komunikatem błędu. Każdy test ma własne UUID i szuka tylko swoich
wiadomości, więc nie przeszkadzają mu dane z poprzednich uruchomień ani
kolejność testów. Testy negatywne sprawdzają też drugi topic: poprawne
zamówienie nie trafia na DLQ, a niepoprawne na `orders-processed`.
Synchronizacja to wyłącznie polling z timeoutem, bez `sleep`.

Stabilność sprawdziłem kilkoma pełnymi przebiegami z rzędu, pojedynczym testem
(`-t`) i w losowej kolejności (`--randomize`). Za każdym razem 21/21.

### Znane ograniczenia / co zrobiłbym inaczej mając więcej czasu

- **Asercja „wiadomość nie przyszła” to okno 5 s.** Nie da fałszywie
  czerwonego wyniku, bo sprawdzamy ją dopiero po dotarciu wyniku na właściwy
  topic. Przy dużym topicu helper może jednak nie zdążyć go przeczytać i test
  przejdzie fałszywie. Lepiej czytać do końcowych offsetów partycji
  (`admin.fetchTopicOffsets`).
- **Wolny start po restarcie `consumer`a (20–25 s).** `consumer` nie obsługuje
  `SIGTERM`, więc zatrzymana instancja nie opuszcza consumer group. Do upływu
  session timeoutu partycje `orders` są przypisane do martwej instancji i nikt
  ich nie przetwarza. Testom nie szkodzi to dzięki readiness checkowi, ale na
  produkcji oznaczałoby przestój przy każdym deployu. Poprawka: graceful
  shutdown, czyli `consumer.disconnect()` po `SIGTERM`.
- **Koszt helperów rośnie z danymi.** Każde wywołanie tworzy nową consumer
  group i czyta topic od offsetu 0. W Kafce zostają osierocone grupy `test-…`.
- **Typowanie helperów.** Zwracają `any` (typy wiadomości DLQ są zdefiniowane
  w testach) i nie zwracają klucza, więc testy nie sprawdzają, że key wyniku
  to `orderId`. `collectMessages` nie wykrywa duplikatów, więc „dokładnie raz”
  nie jest sprawdzane.
- **Timeout helpera zwraca `null` zamiast błędu.** Test kończy się wtedy
  komunikatem `Received: null`, bez informacji, na który topic i jak długo
  czekał. Lepiej, żeby helper rzucał błąd z nazwą topicu i limitem czasu. Przy
  okazji warto wydzielić wspólną pętlę z `waitForMessage` i `collectMessages`,
  które dziś są prawie identyczne.
- **Szum w logach testów.** kafkajs loguje na poziomie INFO każdy start, join
  i stop consumera z helperów, do tego ostrzeżenie o partitionerze. Wystarczy
  `logLevel: logLevel.WARN` i `KAFKAJS_NO_PARTITIONER_WARNING=1`.
- **Inny wyjątek w `eachMessage` nadal zatrzyma przetwarzanie**, np. błąd
  `producer.send` na DLQ. kafkajs go ponawia (`retries: 30`, czyli minuty
  przestoju bez logów), a po restarcie ta sama wiadomość wywoła go znowu.
  Lepiej łapać błędy osobno dla każdej wiadomości i włączyć logi kafkajs na
  poziomie `WARN`.
- **Treść DLQ dla `null`, liczby i booleana nie jest sprawdzana.** Te payloady
  nie mają unikalnego znacznika, a helpery nie zwracają klucza, więc testy
  sprawdzają tylko, że kolejne zamówienie przechodzi. Dla stringa i tablicy
  treść DLQ jest sprawdzana.
- **Luźna walidacja w `consumer`.** Typ `customer` nie jest sprawdzany,
  `amount` może być ujemne, a wejściowy `status` jest ignorowany i nadpisywany
  (testy dokumentują to ostatnie jako obecne zachowanie). Wiadomość bez
  wartości (`value: null`) jest pomijana bez DLQ. Pusty string przechodzi
  natomiast przez `if (!rawValue)`, bo kafkajs zwraca go jako pusty `Buffer`,
  więc według kodu trafia na DLQ jako niepoprawny JSON. Nie ma na to testu.
- **Brak wspólnego kontraktu wiadomości.** Typ zamówienia jest zdefiniowany
  osobno w `producer`, `consumer` i helperach, więc zmiana formatu w
  `consumer` nie zepsuje kompilacji testów. Rozwiązanie: wspólny pakiet typów
  albo schema registry.
- **Testy poza Dockerem nie łączą się z Kafką** (`getaddrinfo ENOTFOUND kafka`).
  Klient łączy się z `localhost:9092`, ale broker podaje mu adres `kafka:9092`
  (`KAFKA_ADVERTISED_LISTENERS`), który istnieje tylko w sieci Dockera.
  Poprawka: drugi listener dla hosta (np. `EXTERNAL://localhost:9094`) albo
  wpis `127.0.0.1 kafka` w `/etc/hosts`. Konfiguracji nie zmieniałem, bo
  wymagana komenda działa.
