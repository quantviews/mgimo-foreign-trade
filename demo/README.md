# trade-demo: демо ИИ-чат по данным

Небольшой сервис, который даёт **демо-доступ к ИИ-ассистенту** по базе внешней
торговли: пользователь регистрируется с корпоративной почты, подтверждает адрес по
ссылке и получает несколько бесплатных диалогов, где ИИ сам обращается к нашим
данным и отвечает обычным языком.

## Как устроено

```
Пользователь → форма регистрации (корпоративный email)
             → письмо со ссылкой → /demo/verify (провижн + сессия-cookie)
             → чат /demo/chat  →  DeepSeek (function calling)
                                     ↕ вызывает наши инструменты
                             data API (trade-api) одним служебным демо-ключом
```

- **Гейт:** только рабочая почта (бесплатные и одноразовые домены отсекаются),
  плюс обязательное подтверждение владения адресом (магик-ссылка).
- **Лимит:** N ходов на пользователя (`demo_access.turn_limit`, по умолчанию 10) +
  общий дневной потолок (`demo_usage_daily`, `MGIMO_DEMO_DAILY_BUDGET`) как бэкстоп.
- **Ключи:** сырые ключи пользователей не хранятся. Инструменты дергают API одним
  служебным ключом на плане `demo` (низкий `max_rows`, только чтение). Пользователю
  свой ключ не нужен.
- **Инструменты:** те же `meta / reference / trade / fizob`, что в MCP, с тем же
  принципом «от общего к частному».

## Эндпоинты

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/demo/register` | `{email, org?}` -> проверка домена -> письмо |
| GET  | `/demo/verify?token=...` | подтверждение -> сессия -> редирект в чат |
| GET  | `/demo/me` | статус сессии и остаток ходов |
| POST | `/demo/chat` | `{messages:[...]}` -> SSE-поток ответа |
| GET  | `/demo/health` | живость |

## Переменные окружения

См. `.env.example`. Обязательные: `MGIMO_DEMO_DEEPSEEK_API_KEY`,
`MGIMO_DEMO_SERVICE_API_KEY` (ключ плана `demo`), `MGIMO_DEMO_POSTGRES_DSN`,
`MGIMO_DEMO_SESSION_SECRET`, а для писем `MGIMO_DEMO_SMTP_*` (отправитель по
умолчанию `research@fief.ru`). Без SMTP ссылка подтверждения пишется в лог (dev).

## Развёртывание (на VPS, рядом с Superset)

```bash
# 1. Миграция (создаёт план demo и таблицы demo_*)
docker exec -i superset-postgres-1 psql -U superset -d tradeapi < api/migrations/004_demo.sql

# 2. Служебный ключ на плане demo (вставить в demo/.env как SERVICE_API_KEY)
docker exec trade-api python -c "import asyncio; from app import store; \
  print(asyncio.run(store.create_user_with_token('demo-service@fief.ru', plan_code='demo')))"

# 3. Заполнить demo/.env (DeepSeek, SMTP, session secret, Postgres DSN, сеть)

# 4. Поднять сервис
cd demo && docker compose up -d --build
```

nginx проксирует `/demo/` на `trade-demo:8000` (SSE: `proxy_buffering off`, длинный
таймаут). Фронтенд чата - страница `site/demo.qmd` (за логином).

## Приватность

Тексты вопросов и подтянутые агрегаты уходят на API DeepSeek (хостинг в КНР). Для
демо по несекретной агрегированной статистике это приемлемо; в интерфейсе чата об
этом есть дисклеймер. Слой инструментов не завязан на провайдера, так что позже
можно переключиться на GigaChat/YandexGPT.
