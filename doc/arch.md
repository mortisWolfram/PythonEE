# Архитектура системы PythonEE

> Модульное ядро, event-driven интеграция через Apache Kafka и расширяемые клиентские приложения.
>
> **Версия:** 1.0
> **Статус:** Draft
> **Дата:** 2026-10-20

## Содержание

- [1. Обзор](#1-обзор)
- [2. Архитектурные драйверы](#2-архитектурные-драйверы)
- [3. Архитектурное решение](#3-архитектурное-решение)
- [4. Компоненты системы](#4-компоненты-системы)
- [5. Kafka: топики, схемы, паттерны](#5-kafka-топики-схемы-паттерны)
- [6. Расширяемость клиентских приложений](#6-расширяемость-клиентских-приложений)
- [7. Безопасность](#7-безопасность)
- [8. Структура репозитория](#8-структура-репозитория)
- [9. Развёртывание](#9-развёртывание)
- [10. Onboarding нового клиента](#10-onboarding-нового-клиента)
- [11. Рекомендации по развитию](#11-рекомендации-по-развитию)

---

## 1. Обзор

Система состоит из **ядра** (Core), где сосредоточена доменная логика платформы, и **клиентских приложений** (личные кабинеты). Первые три — покупатель, продавец, курьер — не являются исчерпывающим списком: архитектура изначально закладывает возможность добавлять новые клиенты (администратор, поддержка, логистика, склад, финансы, white-label порталы) без переписывания ядра.

Вся интеграция между ядром и клиентами, а также между модулями ядра, происходит через **Apache Kafka**. Клиентские приложения не содержат бизнес-правил — они отправляют команды и подписываются на события.

Архитектурный стиль — **event-driven / CQRS-ориентированный модульный монолит** с Kafka в роли backbone.

## 2. Архитектурные драйверы

- **Единый источник истины** для заказов, товаров, доставки, платежей.
- **Слабая связанность**: клиенты и модули ядра знают только топики и схемы, не друг друга.
- **Расширяемость клиентов** — добавление нового кабинета не требует изменений в ядре.
- **Асинхронность по умолчанию** — длительные операции не блокируют интерфейс.
- **Масштабируемость** — consumer groups, партиционирование.
- **Надёжность** — at-least-once + идемпотентность + Outbox + DLQ.
- **Эволюция контрактов** — Schema Registry, версионирование топиков, AsyncAPI.

## 3. Архитектурное решение

### 3.1. Kafka как интеграционная шина

Kafka используется на двух уровнях:

1. **Между клиентами (BFF) и ядром** — команды клиентов и события ядра.
2. **Между модулями ядра** — доменные события и跨-модульные команды.

```
┌──────────────┐   commands    ┌─────────────────────────────────┐
│ Customer BFF │──────────────▶│                                 │
└──────────────┘               │            KAFKA                │
┌──────────────┐   commands    │  (commands / events / replies)  │
│  Seller BFF  │──────────────▶│                                 │
└──────────────┘               └───────────────┬─────────────────┘
┌──────────────┐   commands                    │ events
│ Courier BFF  │──────────────▶                ▼
└──────────────┘               ┌─────────────────────────────────┐
┌──────────────┐   commands    │         CORE (модули)           │
│  Admin BFF   │──────────────▶│  identity │ catalog │ orders    │
└──────────────┘   (new!)      │  delivery │ payments│ notify    │
       ▲                       └─────────────────────────────────┘
       │ events
       └───────────────────────
```

### 3.2. CQRS: команды и запросы

- **Write-сторона** — команды через Kafka. Клиент публикует команду в `*.commands.*`, модуль обрабатывает и публикует результат как событие `*.events.*`.
- **Read-сторона** — **read-модели (materialized views)**, построенные из событий. Клиент читает их через лёгкий query-API (REST/gRPC) поверх read-БД.

Синхронный request-reply через Kafka оставляем только для случаев, где действительно нужен немедленный ответ (например, аутентификация).

### 3.3. Слои внутри модуля ядра

| Слой | Назначение |
|---|---|
| **Domain** | Сущности, агрегаты, value objects, доменные события. Без внешних зависимостей. |
| **Application** | Обработчики команд и событий, Unit of Work. |
| **Infrastructure** | Репозитории (SQLAlchemy), Kafka producer/consumer, Outbox, Schema Registry. |
| **Presentation (Kafka)** | Консьюмеры топиков, маппинг сообщений в команды/события. |

Правило зависимостей: **Presentation → Application → Domain**, Infrastructure реализует интерфейсы.

## 4. Компоненты системы

### 4.1. Ядро (Core)

Модули (bounded contexts):

- **Identity & Access** — пользователи, роли, capabilities, JWT.
- **Catalog** — товары, категории, цены, остатки.
- **Orders** — корзина, оформление, статусы, история.
- **Delivery** — назначение курьера, трекинг, статусы.
- **Payments** — платежи, возвраты, сверки.
- **Notifications** — email/push по событиям.

Публичный контракт модуля = набор топиков (команды и события). Прямые импорты внутренних классов другого модуля запрещены (`import-linter` в CI).

### 4.2. Клиентские приложения

Схема взаимодействия:

```
Frontend (SPA)  ⇄  BFF  ⇄  Kafka  ⇄  Core
   REST/WS        Kafka producer/consumer
```

- **BFF (Backend for Frontend)** — по одному на каждый кабинет. Принимает HTTP/WebSocket от фронтенда, транслирует в команды Kafka, подписывается на события и пушит их клиенту по WebSocket.
- **Фронтенды** — SPA/мобильные, общаются с BFF по REST (команды/запросы) и WebSocket (push событий).

## 5. Kafka: топики, схемы, паттерны

### 5.1. Именование топиков

```
{domain}.{kind}.{name}.v{major}
```

`kind`: `commands` | `events` | `replies` | `dlq`.

Примеры:

- `orders.commands.create.v1`
- `orders.events.order-placed.v1`
- `delivery.events.courier-assigned.v1`
- `orders.replies.v1`

### 5.2. Схемы и конверт

- **Schema Registry** (Confluent / Apicurio) + **Avro** или **Protobuf**.
- Обязательные поля конверта: `messageId`, `correlationId`, `causationId`, `timestamp`, `producer`, `version`, `type`, `tenantId`, `capabilities`.
- Проверка совместимости схем — обязательный шаг CI.

Пример конверта:

```json
{
  "messageId": "b1e5…",
  "correlationId": "a3c2…",
  "causationId": "f9d1…",
  "type": "orders.commands.create",
  "version": 1,
  "timestamp": "2026-10-20T10:00:00Z",
  "producer": "customer-bff",
  "tenantId": "tenant-42",
  "capabilities": ["orders.order.create"],
  "payload": { "customerId": "…", "items": [] }
}
```

### 5.3. Партиционирование

- Ключ = ID агрегата (`orderId`, `productId`, `courierId`) — гарантирует порядок событий по агрегату.
- Число партиций — с запасом (12–24 на топик). Увеличение партиций ломает порядок для существующих ключей — планируйте заранее.

### 5.4. Transactional Outbox

Публикация событий через `outbox`-таблицу и релей (собственный воркер или Debezium CDC). Это гарантирует атомарность «запись в БД + публикация в Kafka».

Схема работы:

1. В той же транзакции, что и изменение агрегата, пишем событие в `outbox`.
2. Релей читает `outbox` и публикует в Kafka, помечая записи отправленными.
3. Гарантия: событие попадёт в Kafka тогда и только тогда, когда зафиксирована транзакция в БД.

### 5.5. Идемпотентность и DLQ

- Все консьюмеры идемпотентны: дедупликация по `messageId` (таблица `processed_messages` или Redis с TTL).
- Проекции обновляются через **upsert** по ключу, не через insert.
- Ошибки повторяются с экспоненциальным backoff, при исчерпании попыток — в `{topic}.dlq.v1` с метаданными об ошибке.
- DLQ мониторится, алерты на рост.

### 5.6. Синхронные операции

- **Команды — всегда через Kafka.**
- **Синхронные запросы — через query-API поверх read-модели** (предпочтительно).
- **Request-reply через Kafka** — только для строго синхронных сценариев (аутентификация). Команда содержит `replyTo` и `correlationId`; ядро публикует ответ в `*.replies.v1`.

## 6. Расширяемость клиентских приложений

Ключевое требование: **добавление нового клиента должно быть конфигурацией + генерацией, а не разработкой с нуля**.

### 6.1. Принципы

1. **Capability-based модель** вместо хардкода ролей.
2. **Declarative client manifest** — клиент описан YAML, а не кодом.
3. **Contract-first (AsyncAPI)** — контракт топиков и команд для каждого клиента.
4. **Shared BFF Kit** — общая библиотека, из которой собирается BFF.
5. **Generator** — шаблон нового BFF и фронтенда.
6. **Plugin system** — кастомизация без форка shared-кода.
7. **Feature flags** — включение функциональности по tenant/role.
8. **Shared frontend kit** — дизайн-система + SDK.

### 6.2. Capability-модель

Вместо фиксированных ролей (`customer`/`seller`/`courier`) вводим два уровня:

- **Role** — метка (`customer`, `seller`, `courier`, `admin`, `support`, `warehouse`, `finance`, …).
- **Capability** — гранулярное право:
  - `catalog.product.read`
  - `catalog.product.write`
  - `orders.order.create`
  - `orders.order.read.own`
  - `orders.status.update`
  - `delivery.tracking.read.own`

Роль = набор capabilities. Клиентское приложение = набор capabilities + набор UI-модулей. Новый клиент = новая композиция capabilities и UI, не новый код в ядре.

### 6.3. Client Manifest

Каждый клиент имеет `clients/{name}-bff/client-manifest.yaml`:

```yaml
name: customer
version: 1.0.0
role: customer

capabilities:
  - catalog.product.read
  - orders.order.create
  - orders.order.read.own
  - orders.order.cancel.own
  - delivery.tracking.read.own

kafka:
  produces:
    - orders.commands.create.v1
    - orders.commands.cancel.v1
  subscribes:
    - orders.events.order-placed.v1
    - orders.events.order-status-changed.v1
    - delivery.events.status-changed.v1
  replies:
    - orders.replies.v1

read_models:
  - id: order_by_id
    path: GET /orders/{id}
    source: orders.read.order_by_id
  - id: product_list
    path: GET /products
    source: catalog.read.product_list

ui:
  theme: customer
  modules:
    - catalog
    - cart
    - orders
    - profile
  routes:
    - /catalog
    - /cart
    - /orders
    - /profile

features:
  - reviews.enabled
  - chat.support.enabled
```

**Этот manifest — единственный источник правды для клиента.** Он драйвит:

- Генерацию **Kafka ACL** (produce/subscribe).
- Генерацию **AsyncAPI-спеки** для клиента.
- Генерацию **TypeScript-типов** для фронтенда.
- Wiring **BFF-рантайма** (какие консьюмеры поднять, какие эндпоинты отдать).
- Документацию в `docs/clients/{name}.md`.

### 6.4. Contract-first через AsyncAPI

Для каждого клиента — `contracts/clients/{name}.asyncapi.yaml`. Спека генерируется из manifest + схем Schema Registry.

Инструменты:

- `asyncapi generate models typescript` → типы для фронтенда.
- `asyncapi generate models python` → модели для BFF.
- `asyncapi generate docs` → документация контракта.

Проверка совместимости AsyncAPI в CI.

### 6.5. Shared BFF Kit

Библиотека `python-ee-bff-kit` содержит всё общее:

- Kafka producer/consumer с обработкой конверта.
- JWT-валидация и извлечение capabilities.
- Пропагация `correlationId`.
- Обработка ошибок, ретраи, DLQ.
- Observability (OpenTelemetry, метрики).
- WebSocket fan-out во фронтенд.
- Загрузчик manifest.
- Middleware: rate limiting, audit.
- Query-клиент к read-моделям.

Новый BFF — тонкий слой:

```python
from bff_kit import build_app

app = build_app(manifest_path="client-manifest.yaml")

# опциональные плагины
app.plugin(AuditPlugin())
app.plugin(RateLimitPlugin(limit=100))
app.plugin(TenantThemePlugin())
```

Кастомная логика добавляется как плагины или отдельные эндпоинты — shared-код не форкается.

### 6.6. Plugin system

BFF-плагины подключаются к lifecycle:

- `on_startup` / `on_shutdown`
- `before_command_publish` / `after_command_publish`
- `on_event_received`
- `before_ws_send`
- `before_query`

Типичные плагины: audit, tenant-специфичные трансформации, дополнительная валидация, обогащение данных для UI.

### 6.7. Generator нового клиента

Шаблоны (через `copier` или `cookiecutter`):

```bash
copier copy gh:python-ee/bff-template clients/admin-bff \
    --data name=admin role=admin
copier copy gh:python-ee/frontend-template frontends/admin-app \
    --data name=admin theme=admin
```

Генерируется:

- `client-manifest.yaml` с базовыми capabilities.
- AsyncAPI-скелет.
- Dockerfile, README, тесты, CI-конфиг.
- Базовые маршруты и WebSocket-обвязка во фронтенде.

Дальше команда уточняет manifest и пишет только специфичную бизнес-логику.

### 6.8. Shared Frontend Kit

NPM-пакет `@python-ee/frontend-kit`:

- Хуки `useKafkaEvents`, `useCommand`, `useCapability`.
- WebSocket-клиент с автопереподключением и восстановлением после реконнекта.
- Типы, сгенерированные из AsyncAPI.
- Дизайн-система (кнопки, таблицы, формы, layout) с темой.
- i18n-обвязка.

Новый фронтенд — `npm create @python-ee/app`, дальше только доменные компоненты.

### 6.9. Feature flags

- Флаги хранятся в отдельном сервисе (собственный модуль `features` или Unleash/Flagsmith).
- Флаги задаются на уровне **tenant / role / user**.
- BFF читает флаги при аутентификации, кеширует, проверяет перед публикацией команды.
- Фронтенд получает флаги вместе с JWT и скрывает недоступные модули.

Это позволяет включать новую функциональность постепенно и не плодить ветки кода.

### 6.10. Multi-tenancy и white-labeling

- `tenantId` в JWT и в конверте сообщений.
- Тема оформления — токены в конфиге клиента (`ui.theme`).
- Feature flags — per tenant.
- Изоляция данных — `tenant_id` в ключевых таблицах + автофильтрация в репозиториях.
- При необходимости — отдельные топики с префиксом `{tenant}.` (редко оправдано).

### 6.11. Пример: добавление Admin-клиента

**Что нужно:**

1. Роль `admin` в Identity + capabilities:
   - `identity.user.manage`
   - `catalog.product.moderate`
   - `orders.order.refund`
   - `disputes.dispute.resolve`
2. `clients/admin-bff/client-manifest.yaml` — produce: `identity.commands.*`, `catalog.commands.moderate`, `orders.commands.refund`; subscribe: все доменные события (с фильтром по tenant).
3. `copier copy` BFF и фронтенда.
4. AsyncAPI `admin.asyncapi.yaml`.
5. Плагин `CrossTenantPlugin` для агрегации данных по всем tenant.
6. Kafka ACL — сгенерированы из manifest.
7. Мониторинг-дашборд и алерты.

**Что НЕ нужно менять:** ядро, схемы событий, другие BFF.

## 7. Безопасность

- **SASL/SSL** к Kafka, **ACL** на уровне топиков: каждый сервис имеет доступ только к своим топикам.
- **JWT** в заголовках сообщений — ядро валидирует токен, роль и capabilities.
- **Проверка владения** (seller меняет только свои товары) — на application-слое.
- **Capability enforcement** — и на BFF (скрытие UI), и на ядре (запрет команды).
- **Аудит** — все команды пишутся в `audit.commands.v1`.

## 8. Структура репозитория

```
python-ee/
├── core/
│   ├── src/
│   │   ├── modules/
│   │   │   ├── identity/       # domain / application / infrastructure / kafka
│   │   │   ├── catalog/
│   │   │   ├── orders/
│   │   │   ├── delivery/
│   │   │   ├── payments/
│   │   │   └── notifications/
│   │   ├── shared/
│   │   │   ├── kafka/
│   │   │   ├── outbox/
│   │   │   ├── idempotency/
│   │   │   └── domain/
│   │   └── main.py
│   ├── tests/
│   ├── alembic/
│   └── pyproject.toml
│
├── packages/
│   ├── bff-kit/                # shared library для всех BFF
│   └── frontend-kit/           # shared npm-пакет
│
├── templates/
│   ├── bff-template/           # copier/cookiecutter
│   └── frontend-template/
│
├── clients/
│   ├── customer-bff/
│   │   ├── client-manifest.yaml
│   │   ├── src/
│   │   └── Dockerfile
│   ├── seller-bff/
│   ├── courier-bff/
│   └── admin-bff/              # добавлен по шаблону
│
├── frontends/
│   ├── customer-app/
│   ├── seller-app/
│   ├── courier-app/
│   └── admin-app/
│
├── contracts/
│   └── clients/
│       ├── customer.asyncapi.yaml
│       ├── seller.asyncapi.yaml
│       ├── courier.asyncapi.yaml
│       └── admin.asyncapi.yaml
│
├── infra/
│   ├── kafka/                  # KRaft, topic definitions, ACLs
│   ├── schema-registry/
│   ├── postgres/
│   └── monitoring/
│
├── docker-compose.yml
└── docs/
    ├── architecture.md         # этот документ
    ├── clients/
    │   ├── customer.md
    │   ├── seller.md
    │   ├── courier.md
    │   └── admin.md
    └── adr/
```

## 9. Развёртывание

- **Kafka** (KRaft) + **Schema Registry** + **Kafka UI** — локально в `docker-compose`.
- **Core** — контейнер(ы) с консьюмерами/продюсерами; масштабируется по партициям.
- **BFF** — отдельные контейнеры, все построены из одного `bff-kit`, отличаются manifest-ом.
- **Frontends** — статика на CDN/Nginx.
- **CI/CD** — общий pipeline, параметризованный manifest-ом.

## 10. Onboarding нового клиента

Чек-лист:

1. Определить роль и набор capabilities.
2. Добавить роль в модуль `identity`.
3. Создать `client-manifest.yaml` в `clients/{name}-bff/`.
4. `copier copy` BFF и фронтенда из шаблонов.
5. Сгенерировать AsyncAPI `contracts/clients/{name}.asyncapi.yaml`.
6. Сгенерировать Kafka ACL из manifest (скрипт в `infra/kafka/acl/generate.py`).
7. Настроить feature flags для tenant/role.
8. Подключить к CI/CD.
9. Добавить дашборд и алерты по лагу/ошибкам.
10. Задокументировать в `docs/clients/{name}.md`.
---

## Приложения

### A. Глоссарий

| Термин | Значение |
|---|---|
| **Bounded context** | Явная граница доменной модели (модуль ядра). |
| **BFF** | Backend for Frontend — сервис, адаптирующий ядро под конкретный клиент. |
| **Capability** | Гранулярное право, проверяемое на ядре и BFF. |
| **Client manifest** | YAML-описание клиента: capabilities, топики, read-модели, UI. |
| **Outbox** | Таблица для атомарной публикации событий вместе с изменением агрегата. |
| **Read-модель** | Проекция, построенная из событий, для чтения клиентами. |
| **Schema Registry** | Хранилище схем сообщений с проверкой совместимости. |
| **Топик** | Канал Kafka, именуемый `{domain}.{kind}.{name}.v{major}`. |

### B. Ссылки

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [AsyncAPI Specification](https://www.asyncapi.com/docs)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Microservices Patterns — Chris Richardson](https://microservices.io/patterns/)
