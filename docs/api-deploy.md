# Деплой API на VPS (рядом со Superset)

> **Статус: развёрнуто и проверено 2026-08-28.** API работает на VPS
> (`http://217.26.28.186`, порт `8090`, HTTP), контейнер `trade-api` в сети
> `superset_default`, токены/аудит — в отдельной БД `tradeapi` того же Postgres,
> DuckDB — `/srv/duckdb/unified_trade_data.duckdb` (read-only). Проверено:
> `/health`, `/v1/trade`, `/v1/meta` + запись аудита. Порт наружу не открыт (пилот,
> без TLS). Обновление ниже — по этому же runbook.

Развёртывание read-only API-сервиса на том же VPS, где крутится Superset
(`http://217.26.28.186:8088`, Superset в Docker). API берёт токены/аудит из Postgres
Superset и читает тот же файл `unified_trade_data.duckdb`. Артефакты — в
[`../api/`](../api/): `Dockerfile`, `docker-compose.yml`, `.env.example`,
`deploy/nginx-api.conf`.

> ⚠️ **TLS пока нет** (домен не готов). На пилоте API работает по **HTTP** на порту `8090`.
> Токены по HTTP идут в открытом виде — **ограничьте доступ фаерволом** до доверенных IP,
> пока не появится домен + TLS. Конфиг nginx уже TLS-ready — включите, когда домен будет.

## 0. Что собрать заранее

- **DSN Postgres Superset** (для схемы `api`): имя сервиса БД в Docker-сети Superset
  (`docker compose ps` в стеке Superset — напр. `db`/`superset_db`), пользователь/пароль/имя БД.
- **Имя Docker-сети Superset**: `docker network ls` (напр. `superset_default`).
- **Путь на хосте к файлу DuckDB**, который читает Superset (напр.
  `/opt/superset/.../unified_trade_data.duckdb`).

## 1. Доставить код на VPS

```bash
ssh user@217.26.28.186
git clone https://github.com/quantviews/mgimo-foreign-trade.git   # или git pull
cd mgimo-foreign-trade/api
cp .env.example .env
nano .env        # заполнить MGIMO_API_POSTGRES_DSN, DUCKDB_HOST_PATH, SUPERSET_NETWORK
```

## 2. Применить миграцию (один раз)

Создаёт схему `api` (plans/users/tokens/audit_log) в Postgres Superset:

```bash
# из каталога api/, с активным .env
set -a; . ./.env; set +a
docker run --rm --network "$SUPERSET_NETWORK" \
  -e MGIMO_API_POSTGRES_DSN="$MGIMO_API_POSTGRES_DSN" \
  -v "$PWD":/app -w /app python:3.12-slim \
  sh -c "pip install -q asyncpg pydantic pydantic-settings && python scripts/init_db.py"
```
(или просто локально на VPS: `pip install asyncpg pydantic-settings && python scripts/init_db.py`.)

## 3. Поднять API

```bash
docker compose up -d --build
docker compose logs -f trade-api        # убедиться, что стартовал и открыл DuckDB
curl -s localhost:8090/health           # -> {"status":"ok",...}
```

## 4. Выдать первый токен

```bash
docker compose exec trade-api python scripts/create_token.py you@org.ru
# печатает 'mgt_...' один раз — сохранить
curl -s -H "Authorization: Bearer mgt_..." \
  "localhost:8090/v1/trade?strana=CN&group_by=period&limit=3"
```

## 5. Безопасность на пилоте (без TLS)

- Открыть порт `8090` только доверенным IP (фаервол/security group) — либо не публиковать
  наружу, а ходить через SSH-туннель, пока нет домена.
- Как появится домен: включить TLS-блок в [`api/deploy/nginx-api.conf`](../api/deploy/nginx-api.conf)
  (Let's Encrypt), проксировать на `127.0.0.1:8090`, порт `8090` наружу закрыть.
- После nginx — учесть `X-Forwarded-For` для реального IP в аудите (сейчас пишется прямой
  `request.client.host`; отмечено TODO в `api/app/audit.py`).

## 6. Обновление данных

При пересборке БД пайплайном: заменить файл DuckDB на хосте (тем же путём) и перезапустить
контейнер, чтобы переоткрыть read-only соединение:
```bash
docker compose restart trade-api
```
(в будущем — эндпоинт `/admin/reload` вместо рестарта.)

## Дашборд «Использование API» (только админам)

Мониторинг использования — на том же Superset, поверх `api.audit_log`.

**1. Применить view'ы** (один раз; `002_usage_views.sql` создаёт `api.usage_log` и
`api.usage_by_user_month`):
```bash
cd ~/mgimo-foreign-trade && git pull
docker exec -i superset-postgres-1 psql -U superset -d tradeapi \
  < ~/mgimo-foreign-trade/api/migrations/002_usage_views.sql
```

**2. Подключить БД `tradeapi` в Superset** (под учёткой Admin): Settings → **Database
Connections** → **+ Database** → PostgreSQL:
- Host `postgres`, Port `5432`, Database `tradeapi`, User `superset`, Password `superset`.
- Display name: `tradeapi`. Test → Connect.

**3. Датасеты:** Datasets → **+ Dataset** → Database `tradeapi`, Schema `api`, таблица
`usage_log` (детальный лог) и/или `usage_by_user_month` (сводка). Create.

**4. Дашборд:** собрать чарты (по `usage_log`): запросы по `email`/по `endpoint`, динамика
по `day`, доля `is_error`, объём `rows_returned`, латентность. Сохранить дашборд
«Использование API».

**5. Ограничить доступ админам.** Новая БД `tradeapi` и её датасеты по умолчанию доступны
**только Admin** (и владельцам) — просто **не выдавайте** права на неё ролям `API`/Gamma.
Дополнительно на дашборде: **Edit properties → Roles → Admin** (чтобы он не появлялся в
списках у остальных). Так дашборд видят только администраторы.

> `usage_log` также включает `params` (JSON) и `ip` — при желании убрать из датасета
> лишние столбцы в его настройках.

## Не коммитить

`api/.env` (DSN/пароли) — только `.env.example`. Файл DuckDB и `.env` в репозиторий не
попадают.
