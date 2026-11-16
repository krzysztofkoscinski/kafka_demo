# Kafka Demo

Prosty projekt demonstracyjny pokazujący komunikację między usługami przy użyciu Kafki. Celem tego repozytorium jest przedstawienie podstawowego przepływu zdarzeń w architekturze event-driven oraz pokazanie, jak testować takie scenariusze w praktyce.

## Cel projektu

Projekt ma za zadanie zademonstrować:
- wysyłanie wiadomości do topicu w Apache Kafka,
- odbieranie wiadomości przez konsumenta,
- prostą integrację między serwisami,
- testy integracyjne sprawdzające rzeczywistą komunikację przez brokera wiadomości,
- dobry wzorzec do dalszej rozbudowy o retry, idempotencję i obsługę błędów.

## Architektura

W najprostszej wersji projekt składa się z:
- producenta (producer), który wysyła zdarzenie do Kafki,
- topicu `orders` lub podobnego,
- konsumenta, który odbiera wiadomość i wykonuje dalszą logikę biznesową.

Przykładowy przepływ:
1. Aplikacja wysyła żądanie utworzenia zamówienia.
2. Producent publikuje zdarzenie do Kafki.
3. Konsument pobiera wiadomość z topicu.
4. Następuje przetworzenie danych i zapis wyniku lub wykonanie dalszej operacji.

## Technologie

- Java
- Spring Boot
- Spring Kafka
- Apache Kafka
- Maven / Gradle
- JUnit 5
- Testcontainers (dla testów integracyjnych)
- Docker / Docker Compose

## Wymagania

Aby uruchomić projekt lokalnie, potrzebujesz:
- JDK 17+
- Maven lub Gradle
- Docker i Docker Compose
- Git

## Uruchomienie projektu

### 1. Uruchom Kafka lokalnie

Najprościej użyć Docker Compose:

```bash
docker-compose up -d
```

Jeśli w repo nie ma pliku `docker-compose.yml`, można uruchomić Kafkę za pomocą oficjalnego obrazu Docker:
```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:latest
```

### 2. Skompiluj projekt

```bash
mvn clean install
```

### 3. Uruchom aplikację

```bash
mvn spring-boot:run
```

## Przykładowy scenariusz

Przykład biznesowy:
- użytkownik składa zamówienie,
- system wysyła zdarzenie `OrderCreated`,
- drugi serwis odbiera je i generuje płatność, zapisuje status lub wysyła powiadomienie.

Przykładowy payload:
```json
{
  "orderId": "1001",
  "userId": "42",
  "amount": 149.99,
  "createdAt": "2026-11-16T12:00:00"
}
```

## Configuracja

W aplikacji należy ustawić podstawowe parametry połączenia do Kafki, np.:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=demo-group
spring.kafka.consumer.auto-offset-reset=earliest
```

W razie potrzeby można dodać:
- topic prefixes,
- retry configuration,
- DLQ (Dead Letter Topic),
- custom serializer/deserializer.

## Testy

Projekt powinien zawierać testy integracyjne z użyciem Testcontainers, które uruchamiają rzeczywistą instancję Kafki w kontenerze.

### Typy testów
- test jednostkowy biznesowy,
- test producenta (czy wiadomość trafia do topicu),
- test konsumenta (czy wiadomość jest poprawnie przetwarzana),
- test end-to-end (pełny przepływ od żądania do obsługi zdarzenia).

### Przykład testu integracyjnego
Test powinien:
1. uruchomić kontener z Kafka,
2. wysłać żądanie do serwisu,
3. poczekać aż wiadomość pojawi się w topicu,
4. sprawdzić poprawność payloadu,
5. upewnić się, że event nie został uszkodzony.

## Struktura projektu

```text
kafka-demo/
├── src/
│   ├── main/
│   │   ├── java/
│   │   └── resources/
│   └── test/
│       └── java/
├── docker-compose.yml
├── pom.xml
├── README.md
└── .gitignore
```

## Wartości biznesowe projektu

Projekt pokazuje, że potrafię pracować z:
- asynchroniczną komunikacją między usługami,
- brokerem wiadomości,
- równoległym przetwarzaniem zdarzeń,
- testami integracyjnymi opartymi na rzeczywistym środowisku,
- wzorcami związanymi z niezawodnością i obsługą błędów.

## Dalsze rozszerzenia

W kolejnych krokach można rozbudować projekt o:
- retry policy,
- idempotent consumer,
- DLQ (Dead Letter Topic),
- observability i logowanie,
- walidację eventów,
- testy dla duplikatów i błędów przetwarzania,
- dodatkowe serwisy i bardziej realistyczny flow domenowy.

## Podsumowanie

To repozytorium jest dobrym przykładem małego, czytelnego projektu demonstracyjnego dla portfolio Java/Kafka. Pokazuje praktyczne umiejętności związane z komunikacją asynchroniczną oraz testowaniem integracyjnym działań opartych na brokerze wiadomości.

---

Jeśli chcesz, mogę od razu przygotować Ci wersję README bardziej:
- biznesową,
- techniczną,
- portfolio-ready,
- albo dopasowaną do konkretnego projektu Java/Spring Boot z nazwami klas i endpointów.
