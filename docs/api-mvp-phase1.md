# API MVP — Фаза 1 (детальная спецификация)

> Составлено: 2026-07-30. Родительский план: [api-plan.md](api-plan.md).
> Цель Фазы 1: рабочий API по торговым данным с токен-авторизацией, аудитом (метринг с
> первого дня) и потреблением из Excel. Квоты/биллинг/OData/fizob — следующие фазы.

> **Статус: бэкенд реализован** — код в [`../api/`](../api/), справочник для потребителей —
> [api-reference.md](api-reference.md). Готово: `/health`, `/v1/meta`, `/v1/reference/*`,
> `/v1/trade` (JSON/CSV, агрегация+сырьё), Bearer+Basic, Postgres-стор
> (plans/users/tokens/audit) + миграция + скрипты, аудит с `cost_units` — проверено
> end-to-end на живом Postgres, задеплоено на VPS. Осталось довести: кабинет в Superset
> (роль-допуск, БЕЗ саморегистрации — см. `superset_cabinet/`), keyset-cursor экспорт,
> enforcement квот/rate-limit.

## Границы Фазы 1

**В объёме:** FastAPI поверх read-only DuckDB; статические токены (Bearer + Basic);
эндпоинты `/v1/trade`, `/v1/reference/*`, `/v1/meta`, `/health`; аудит-лог с `cost_units`;
таблицы `plans`/`api_users`/`api_tokens` (все на плане «pilot»); кабинет-страница в Superset;
кабинет-страница в Superset (роль-допуск для существующих пользователей, без
саморегистрации); инструкция Power Query.

**НЕ в объёме (следующие фазы):** enforcement квот и rate-limit (пока только метрим);
биллинг/оплата; OData-фид; эндпоинты fizob; админ-UI сверх само-сервиса.

## Схема Postgres (DDL)

```sql
-- Тарифы: права как данные
CREATE TABLE plans (
    id                 SERIAL PRIMARY KEY,
    code               TEXT UNIQUE NOT NULL,        -- 'pilot','free','pro'
    name               TEXT NOT NULL,
    rate_limit_per_min INT,                         -- NULL = без лимита (Фаза 2)
    monthly_quota      INT,                         -- NULL = без лимита (Фаза 2)
    max_rows           INT NOT NULL DEFAULT 100000, -- потолок строк на ответ
    scopes             TEXT[] NOT NULL DEFAULT '{trade:read}',
    active             BOOLEAN NOT NULL DEFAULT TRUE
);
INSERT INTO plans(code,name,max_rows,scopes)
VALUES ('pilot','Pilot (unlimited)',1000000,'{trade:read,fizob:read}');

-- Пользователи API (связаны с учёткой Superset/FAB ab_user)
CREATE TABLE api_users (
    id               SERIAL PRIMARY KEY,
    superset_user_id INT,                           -- ab_user.id
    email            TEXT UNIQUE NOT NULL,
    org              TEXT,
    plan_id          INT NOT NULL REFERENCES plans(id),
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Токены: хранятся ХЕШИРОВАННЫМИ
CREATE TABLE api_tokens (
    id           SERIAL PRIMARY KEY,
    user_id      INT NOT NULL REFERENCES api_users(id),
    token_hash   TEXT NOT NULL,                     -- sha256(raw token)
    prefix       TEXT NOT NULL,                     -- первые ~8 симв., для показа
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);
CREATE INDEX ix_api_tokens_hash ON api_tokens(token_hash);

-- Аудит-лог: истина для мониторинга и будущего биллинга
CREATE TABLE api_audit_log (
    id            BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id       INT,                              -- NULL для отклонённых
    token_id      INT,
    endpoint      TEXT NOT NULL,
    method        TEXT NOT NULL,
    params        JSONB,
    status        INT NOT NULL,
    rows_returned INT,
    bytes         INT,
    latency_ms    INT,
    cost_units    INT NOT NULL DEFAULT 1,           -- измерение тарификации
    ip            INET
);
CREATE INDEX ix_audit_user_ts ON api_audit_log(user_id, ts);
```

Отдельная схема (напр. `api`) в том же Postgres, что у Superset.

## Эндпоинты Фазы 1

Базовый префикс `/v1`. Все, кроме `/health`, требуют токен. Ответ по умолчанию JSON;
`?format=csv` — CSV с UTF-8 BOM и разделителем `;`.

### `GET /health` (без авторизации)
 Liveness + версия данных: `{"status":"ok","data_version":"2026-06","rows":7416603}`.

### `GET /v1/meta`
Доступные измерения, диапазон дат, версия данных, сводка твоего плана и использования за месяц.

### `GET /v1/reference/countries`
`[{"strana":"CN","country_name":"Китай"}, ...]` — для выпадашек.

### `GET /v1/reference/tnved?level=2|4|6|8|10`
`[{"code":"27","name":"Топливо минеральное..."}, ...]`.

### `GET /v1/trade` — основной эндпоинт
Параметры (все опциональны):

| Параметр | Пример | Смысл |
|---|---|---|
| `strana` | `CN` (повторяемый) | фильтр по стране (ISO) |
| `napr` | `im` / `ex` (или `ИМ`/`ЭК`) | направление; ASCII-алиасы — основной путь |
| `type` | `fact` / `pred` | факт vs прогноз |
| `source` | `national`/`comtrade`/`nowcast` | источник |
| `tnved2/tnved4/tnved6` | `27` | фильтр по уровню кода |
| `edizm_iso` | `166` (повторяемый) | доп. единица по ISO-коду — основной фильтр |
| `edizm` | `ШТУКА` | доп. единица по названию — тоже принимается |
| `period_from`,`period_to` | `2024-01` | диапазон периодов (`YYYY-MM` или дата) |
| `group_by` | `strana,tnved2,period` | измерения агрегации |
| `metrics` | `stoim,netto` (по умолч.) | меры; `kol` — только явно (требует `edizm` в group_by) |
| `include` | `tnved4_name,country_name` | доп. поля-названия в ответе (opt-in) |
| `format` | `json`/`csv` | формат (JSON — рекомендуется) |
| `limit`,`cursor` | `10000` | размер страницы (`≤ plan.max_rows`) + keyset-курсор для экспорта |
| `order_by` | `period` | сортировка |

**Единицы измерения (`EDIZM`).** Фильтровать **по `edizm_iso`** (числовой ISO — устойчиво в URL,
основной путь); `edizm` по названию тоже принимается, но кириллица в URL менее удобна. Оба поля
(`edizm` название + `edizm_iso`) отдаются в ответе — без них `KOL` (количество в доп. единице) не
интерпретируется. **Важно:** `KOL` аддитивен только внутри одной `EDIZM` (штуки+литры
складывать нельзя — тот же принцип, что в fizob-агрегатах). Поэтому при `metrics`,
содержащих `kol`, в агрегации **`edizm` обязана быть в `group_by`** — иначе `400` (или
авто-добавляем `edizm` в группировку), чтобы не суммировать разные единицы. На `stoim`/`netto`
это не распространяется (USD и кг аддитивны).

**Названия кодов (`include`).** По умолчанию ответ «лёгкий» — только коды. Параметр `include`
подтягивает названия: `country_name`, `tnved2_name`, `tnved4_name`, `tnved6_name`, `tnved_name`
(и `edizm`/`edizm_iso`, если не в group_by). Реализация ложится на гибрид: **без `include`
читаем базовую `unified_trade_data`** (быстро), **с `include` — `unified_trade_data_enriched`**
(view с джойнами справочников). Список допустимых значений `include` — по allowlist.

Пример (с названием и корректной группировкой `kol` по `edizm`):
```
GET /v1/trade?strana=CN&napr=im&period_from=2024-01
    &group_by=tnved2,edizm,period&metrics=stoim,netto,kol&include=tnved2_name&format=json
```
Ответ:
```json
{
  "meta": {"rows": 240, "truncated": false, "max_rows": 1000000},
  "data": [
    {"tnved2":"85","tnved2_name":"Электрические машины...","edizm":"ШТУКА",
     "period":"2024-01-01","stoim":1234.5,"netto":67.8,"kol":9.0}, ...
  ]
}
```
(Без `edizm` в `group_by` при `metrics=...,kol` — `400`: `kol` нельзя суммировать по разным
единицам.)

**Безопасность запроса:** имена колонок для фильтров/`group_by`/`order_by` — по **allowlist**
(маппинг разрешённое-имя → колонка); значения — только через **параметризованные** запросы к
DuckDB. Сырой SQL от пользователя в v1 НЕ принимаем.

## Ограничения и правила запросов (решения 2026-07-30)

- **Сырые строки (без `group_by`) — разрешены, но с потолком** `plan.max_rows` на страницу.
  Полная выгрузка датасета — не для free-плана: объём/число страниц ограничены тарифом
  (в пилоте — щедро). Это защищает и процесс, и монетизацию (free не должен «выкачать всё»).
- **Экспорт больших объёмов — keyset/cursor-пагинация** (по `period` + ключ), не `offset`
  (медленно/нестабильно на больших данных). `limit` — потолок страницы; сколько страниц/данных
  доступно — по тарифу. Ответ отдаёт `next_cursor`, пока есть данные.
- **Формат чисел / CSV.** Рекомендуем **JSON + Power Query** (типы, UTF-8, без разделителей).
  CSV — опция; числа отдаём с точкой-десятичным и **явно это документируем** (ru-Excel ждёт
  запятую — ещё одна причина вести на JSON). CSV — с UTF-8 BOM и разделителем `;`.
- **Защита процесса (с Фазы 1, это не тарифная квота):** мягкий технический rate-limit +
  **таймаут на запрос** + cap строк. DuckDB-процесс один — беречь от runaway-клиента.
- **Ошибки** — формат `application/problem+json` (RFC 7807): `type`, `title`, `status`, `detail`.
- **Периоды:** на входе принимаем `YYYY-MM` и полную дату; на выходе отдаём `YYYY-MM`.
- **Версия данных** (`/health`, `/meta`) = максимальный `PERIOD` + timestamp сборки.
- **CORS** — включаем только если появится браузерный фронт; для Excel/скриптов не нужен.

## Авторизация (реализация)

1. Извлечь токен: **только из заголовков** — `Authorization: Bearer <token>` **или** Basic-auth
   (`password=<token>`, username любой) — Basic нужен для Power Query. Токен в query-параметре
   (`?token=`) **не поддерживаем** (утечёт в логи/аудит).
2. `sha256(token)` → поиск в `api_tokens` по `token_hash`: не отозван, не истёк → `api_users`
   → `plans` активен.
3. Успех: положить `user`/`plan` в контекст запроса; `last_used_at` обновлять батчем/асинхронно.
4. Неуспех: `401`.
5. Фаза 1: квоты/rate-limit **не enforced** (план «pilot»), но аудит пишет `cost_units` —
   метрика копится для будущего биллинга.

## Аудит (middleware)

Обёртка вокруг каждого запроса: замер времени → запись в `api_audit_log`
(`user_id, endpoint, method, params, status, rows_returned, bytes, latency_ms, cost_units=1, ip`).
Запись **асинхронная/фоновая** (не блокировать ответ; батч-инсерт). `params` — санитизированные
(без токена).

## Кабинет в Superset (FAB custom view)

- Blueprint/BaseView на маршруте `/cabinet/api-key` внутри Superset.
- Залогиненный пользователь видит: префикс своего токена (маскированный), кнопку
  «Перевыпустить», свой план, «использовано N запросов за месяц» (COUNT из `api_audit_log`).
- «Перевыпустить»: генерим токен (`mgt_` + 32 urlsafe симв.), пишем `sha256` + `prefix`,
  **показываем сырой один раз**.
- Саморегистрация: включить FAB `AUTH_USER_REGISTRATION=True` + email-подтверждение;
  дефолтная роль — **минимальная** (только кабинет; НЕ дашборды/SQL Lab).

## Инструкция для аналитиков (Power Query)

1. Excel → **Данные → Получить данные → Из интернета**.
2. URL, напр.:
   `https://api.<домен>/v1/trade?strana=CN&napr=ИМ&period_from=2024-01-01&format=json`
3. Тип доступа — **Основной (Basic)**: имя `token`, пароль — **ваш API-ключ** (из кабинета).
4. Power Query развернёт JSON в таблицу (типы сохранены, UTF-8 — без проблем с разделителями).
   Обновление — кнопкой «Обновить».

## Структура сервиса (FastAPI)

```
api/
  app/
    main.py            # FastAPI + lifespan: открыть DuckDB read-only, Postgres pool
    config.py          # настройки: путь к duckdb, DSN Postgres, домен, CORS
    db.py              # DuckDB read-only (cursor на запрос); asyncpg pool
    auth.py            # Depends: разбор+валидация токена (Bearer/Basic)
    audit.py           # middleware + фоновый писатель в api_audit_log
    query.py           # безопасный билдер фильтр→SQL (allowlist + параметры)
    routers/{trade,reference,meta,health}.py
    models.py          # Pydantic: query-параметры и ответы
  requirements.txt / Dockerfile
```

Зависимости: `fastapi`, `uvicorn[standard]`, `gunicorn`, `duckdb`, `asyncpg` (или
`psycopg`), `pydantic`.

## Развёртывание

- `gunicorn` с воркерами `uvicorn` за существующим `nginx` (**TLS готов**).
- DuckDB открывается read-only на старте (lifespan); на запрос — `.cursor()` (независимые
  курсоры на одном соединении) или маленький пул read-only соединений. Много читателей — ок.
- **Своп файла при пересборке:** пайплайн кладёт новый `unified_trade_data.duckdb` → сигнал
  сервису переоткрыть коннекты. В Фазе 1 достаточно эндпоинта `/admin/reload` (токен админа)
  или перезапуска; авто-watch по mtime — позже.
- Postgres — тот же инстанс, что у Superset (схема `api`).

## Критерии готовности Фазы 1

- Пользователь саморегистрируется в Superset, в кабинете выпускает токен.
- `GET /v1/trade` с токеном отдаёт отфильтрованные данные в JSON и CSV.
- Excel через Power Query + Basic-auth тянет и обновляет данные.
- Каждый запрос попадает в `api_audit_log` с `cost_units`.
- Всё по HTTPS; сырой SQL недоступен; `limit` ограничен `plan.max_rows`.

## Открытые мелочи к реализации

- Формат токена и срок жизни (дефолт: `mgt_`+32 симв., без истечения до Фазы 2).
- Точные allowlist'ы: измерения `group_by`, поля `include` (названия), меры `metrics`.
- `cost_units`: в Фазе 1 = 1/запрос; пересмотреть при выборе тарифной единицы.
