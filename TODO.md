# Plan i stan realizacji

## Status

- **Etapy 1–7 — zrealizowane i zweryfikowane.**
- Projekt ma moduły Maven `contracts`, `order-service`, `billing-service` i `e2e-tests`.
- Kontrakt: `eventId`, `orderId`, `createdAt`, `amount`; topic `orders`; DLT `orders.DLT`.
- Pełne `mvn --batch-mode verify`: 10 testów, 0 błędów, 0 wyjątków.
- `docker compose up -d --wait kafka`: broker zgłosił status `healthy`; kontener został następnie zatrzymany.

## Etapy

### 1. Dwa serwisy i zdarzenie — gotowe

- [x] Niezależne Order Service i Billing Service.
- [x] `POST /orders` z walidacją i publikacją `OrderCreated`.
- [x] Wspólny kontrakt, klucz wiadomości `orderId` oraz topic `orders`.
- [x] Konsument Billing i repozytorium płatności w pamięci.

### 2. Testy jednostkowe — gotowe

- [x] Test tworzenia zamówienia i publikowanego zdarzenia.
- [x] Test mapowania zdarzenia na płatność.

### 3. Testy integracyjne producenta — gotowe

- [x] Kafka w Testcontainers; asercje odpowiedzi HTTP, topicu, klucza i payloadu.
- [x] Odrzucenie niepoprawnego żądania.
- [x] Unikalne identyfikatory zamówień i grupy konsumentów testowych.

### 4. Testy integracyjne konsumenta — gotowe

- [x] Poprawne przetwarzanie i asercje asynchroniczne z Awaitility.
- [x] Ograniczone timeouty bez `Thread.sleep()`.
- [x] Błędne payloady i weryfikacja dalszego przetwarzania kolejki.

### 5. Retry, DLT i idempotencja — gotowe

- [x] Dwie ponowne próby po 100 ms (maksymalnie trzy przetworzenia łącznie).
- [x] DLT `orders.DLT` po wyczerpaniu prób i dla niepoprawnych payloadów.
- [x] Idempotencja oparta o `eventId`, sprawdzona testem duplikatu.
- [x] Testy przejściowego błędu, wyczerpania retry, DLT i nieznanego pola.
- [x] Udokumentowanie ograniczeń trwałości i gwarancji kolejności.

### 6. E2E i CI — gotowe

- [x] Test od API Order Service przez Kafkę do płatności w Billing Service.
- [x] GitHub Actions uruchamia `mvn --batch-mode verify` na PR i pushu do `main`.
- [x] Testy zarządzają własną Kafką przez Testcontainers.

### 7. Dokumentacja i uruchamianie — gotowe

- [x] README z architekturą, komendami, tabelą testów i wynikiem pełnego uruchomienia.
- [x] `docs/testing-strategy.md` opisuje schemat, retry, DLT, duplikaty i ograniczenia.
- [x] Docker Compose dla lokalnego brokera; konfiguracja sprawdzona healthcheckiem.
- [x] Plik `.gitignore` wyklucza katalogi `target/` wszystkich modułów.

## Kryteria ukończenia

- [x] Projekt i lokalny broker można uruchomić zgodnie z README.
- [x] Testy integracyjne i E2E sprawdzają prawdziwy broker testowy.
- [x] Błędy, duplikaty, retry i DLT są udokumentowane oraz pokryte testami.
- [x] Asercje asynchroniczne są ograniczone timeoutem, bez stałych opóźnień.
- [x] CI buduje i testuje projekt.
