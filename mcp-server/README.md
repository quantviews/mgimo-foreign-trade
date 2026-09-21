# MGIMO trade - MCP-сервер

[MCP](https://modelcontextprotocol.io)-сервер, который отдаёт API внешней
торговли МГИМО (`https://nts.mgimo.ru/api`) в виде инструментов, чтобы любой
MCP-клиент (Claude Desktop, IDE, агенты) мог отвечать на вопросы по данным
обычным языком. Это тонкая обёртка над HTTP API: авторизация, квоты и аудит
остаются на стороне API.

Работает в двух режимах: **локально (stdio)** - клиент сам запускает сервер, ключ
в переменной окружения; и **удалённо (streamable-http)** - хостится на VPS, ключ
идёт в заголовке каждого запроса.

## Инструменты

| Инструмент | Что делает |
|---|---|
| `meta` | последний доступный период + ваш тариф и остаток квоты |
| `reference` | справочники кодов: `countries` (ISO-2 + русские названия) или `tnved` (названия кодов ТН ВЭД, по `level`) |
| `trade` | строки торговли или агрегаты на сервере (фильтры: страна, направление, код ТН ВЭД, период, `group_by`, `metrics`) |
| `fizob` | индексы физобъёмов (реальные объёмы без влияния цен - особенность проекта) |

## Требования

- Python 3.10+.
- Персональный API-ключ. Получить в кабинете Superset:
  <https://nts.mgimo.ru/superset/apikey/> (нужна роль `API`, назначает админ).

## Установка

Через [uv](https://docs.astral.sh/uv/) (рекомендуется, без ручного venv):

```bash
cd mcp-server
uv sync
```

Или обычный pip в виртуальном окружении:

```bash
cd mcp-server
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Подключение MCP-клиента (локально, stdio)

Клиент запускает `server.py`, ключ передаётся в `env`. Токен уходит только в
заголовок Authorization и нигде не печатается.

**Claude Desktop** - откройте `claude_desktop_config.json`
(macOS: `~/Library/Application Support/Claude/`, Windows: `%APPDATA%\Claude\`),
добавьте сервер и перезапустите Claude Desktop:

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "uv",
      "args": ["run", "--directory", "/АБСОЛЮТНЫЙ/ПУТЬ/К/mcp-server", "mgimo-trade-mcp"],
      "env": { "MGIMO_API_TOKEN": "mgt_ВАШ_ТОКЕН" }
    }
  }
}
```

Без uv - запускайте скрипт напрямую тем Python, где установлены зависимости:

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "/АБСОЛЮТНЫЙ/ПУТЬ/К/mcp-server/.venv/bin/python",
      "args": ["/АБСОЛЮТНЫЙ/ПУТЬ/К/mcp-server/server.py"],
      "env": { "MGIMO_API_TOKEN": "mgt_ВАШ_ТОКЕН" }
    }
  }
}
```

На Windows укажите полный путь к `python.exe` и слэши в путях, например
`C:\\...\\.venv\\Scripts\\python.exe`.

Другие MCP-клиенты (Cursor, Continue и т.п.) настраиваются так же: stdio-сервер,
запускаемый этой командой, и `MGIMO_API_TOKEN` в окружении.

## Удалённый (хостинг) сервер

На VPS работает готовый экземпляр по адресу **`https://nts.mgimo.ru/mcp`**
(транспорт Streamable HTTP, TLS). Ставить ничего не нужно: каждый запрос должен
нести персональный ключ клиента в `Authorization: Bearer <ключ>`, который сервер
прокидывает в API. Поэтому квоты и аудит считаются по каждому пользователю, а сам
сервер токен не хранит.

### Подключение ИИ-клиентов (удалённо)

Всем клиентам нужно одно и то же: транспорт **streamable-http**, URL
**`https://nts.mgimo.ru/mcp`** и ваш ключ в заголовке **`Authorization: Bearer`**.
Сначала получите ключ в кабинете Superset:
<https://nts.mgimo.ru/superset/apikey/>. После подключения клиент увидит
инструменты `meta`, `reference`, `trade`, `fizob`.

**Claude Desktop** - для удалённых серверов он ждёт OAuth, поэтому bearer-ключ
проще подключить через мост [`mcp-remote`](https://www.npmjs.com/package/mcp-remote)
(нужен Node.js). Откройте `claude_desktop_config.json` (Windows `%APPDATA%\Claude\`,
macOS `~/Library/Application Support/Claude/`) и перезапустите:

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://nts.mgimo.ru/mcp",
               "--header", "Authorization: Bearer mgt_ВАШ_ТОКЕН"]
    }
  }
}
```

**Claude Code (CLI)** - умеет удалённый HTTP MCP с заголовком напрямую:

```bash
claude mcp add --transport http mgimo-trade https://nts.mgimo.ru/mcp \
  --header "Authorization: Bearer mgt_ВАШ_ТОКЕН"
```

**Cursor** - удалённый MCP через `url` + `headers`. Два способа, любой на выбор.

1. *Через файл конфигурации.* Создайте `.cursor/mcp.json` в корне проекта (сервер
   будет только в этом проекте) или `~/.cursor/mcp.json` (глобально, во всех
   проектах):

   ```json
   {
     "mcpServers": {
       "mgimo-trade": {
         "url": "https://nts.mgimo.ru/mcp",
         "headers": { "Authorization": "Bearer mgt_ВАШ_ТОКЕН" }
       }
     }
   }
   ```

2. *Через интерфейс.* Cursor Settings (`Ctrl/Cmd+Shift+J`) → раздел **MCP**
   (в новых версиях **Tools & Integrations**) → **New MCP Server** / **Add** →
   вставьте тот же JSON и сохраните.

После этого в **Settings → MCP** сервер `mgimo-trade` должен загореться зелёным и
показать инструменты `meta`, `reference`, `trade`, `fizob` (если серый - нажмите
кнопку обновления или проверьте ключ и интернет). Пользоваться в чате в режиме
**Agent**: ассистент сам вызывает инструменты; при первом вызове Cursor может
попросить подтвердить запуск - разрешите (можно включить «always allow» для этого
сервера). Токен хранится в файле конфигурации в открытом виде, поэтому
`.cursor/mcp.json` в проекте лучше не коммитить в общий репозиторий.

**VS Code** (Copilot, режим Agent) - формат отличается: файл `.vscode/mcp.json`,
ключ `servers` и `type: http`:

```json
{
  "servers": {
    "mgimo-trade": {
      "type": "http",
      "url": "https://nts.mgimo.ru/mcp",
      "headers": { "Authorization": "Bearer mgt_ВАШ_ТОКЕН" }
    }
  }
}
```

**Свой агент (Python)** - или любой MCP-совместимый фреймворк (адаптеры
LangChain / LlamaIndex, собственный цикл):

```python
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async with streamablehttp_client(
        "https://nts.mgimo.ru/mcp",
        headers={"Authorization": "Bearer mgt_ВАШ_ТОКЕН"}) as (r, w, _):
    async with ClientSession(r, w) as s:
        await s.initialize()
        print([t.name for t in (await s.list_tools()).tools])
        print((await s.call_tool("meta", {})).content[0].text)
```

**ChatGPT** - пока напрямую подключить нельзя. ChatGPT умеет удалённые
MCP-серверы (Connectors / Developer Mode на платных тарифах), но при добавлении
коннектора предлагает только **OAuth** или **без авторизации**: поля для
статического заголовка `Authorization: Bearer` там нет, а наш сервер требует ключ
именно в заголовке. Чтобы заработало, в сервер нужно добавить OAuth 2.1 (в
планах). Клиенты, которые умеют произвольный заголовок (Claude Desktop/Code,
Cursor, VS Code, свой агент), работают сразу.

Хотите запускать у себя, а не через хостинг? Используйте локальный stdio-режим
выше (["Подключение MCP-клиента"](#подключение-mcp-клиента-локально-stdio)) с
`MGIMO_API_TOKEN` в окружении.

### Деплой / обновление хостинг-сервера (VPS)

Сервер работает контейнером `trade-mcp` рядом с `trade-api`, в Docker-сети
Superset, за nginx контейнера `landing` на `/mcp` (см. `deploy/landing-nginx.conf`).
API он зовёт внутренне (`trade-api:8000`) и токен не хранит.

```bash
ssh mgimo
cd ~/mgimo-foreign-trade && git pull
cd mcp-server && docker compose up -d --build     # собрать + (пере)запустить trade-mcp
```

Локация `/mcp` в nginx уже есть; после её правки:
`scp deploy/landing-nginx.conf mgimo:/home/marcel/landing-nginx.conf && \
 ssh mgimo "docker exec landing nginx -t && docker exec landing nginx -s reload"`.

Проверка: `curl -sN -H "Authorization: Bearer mgt_..." https://nts.mgimo.ru/mcp`
должен говорить на MCP (голый GET отдаёт 406/JSON - это нормально; вызывать
инструменты нужно настоящим клиентом).

## Переменные окружения

| Переменная | По умолчанию | Примечание |
|---|---|---|
| `MGIMO_API_TOKEN` | нет | обязательна в stdio-режиме; персональный ключ |
| `MGIMO_API_BASE` | `https://nts.mgimo.ru/api` | переопределение для локального/dev API |
| `MCP_TRANSPORT` | `stdio` | `stdio`, `streamable-http` или `sse` |
| `MCP_HOST` | `127.0.0.1` | адрес привязки для http-транспортов |
| `MCP_PORT` | `8000` | порт для http-транспортов |

В stdio-режиме, как запасной вариант, сервер также читает строку
`MGIMO_API_TOKEN=` из `.env` рядом с `server.py` или в рабочем каталоге - удобно
для локальных тестов. Такой `.env` в репозиторий не коммитить.

## Быстрая проверка

Запустите сервер отдельно - он стартует и ждёт на stdio (Ctrl+C для остановки):

```bash
MGIMO_API_TOKEN=mgt_... uv run mgimo-trade-mcp
```

Чтобы проверить путь к API без клиента, есть соседний
[`.claude/skills/trade-data/query.py`](../.claude/skills/trade-data/query.py)
(те же эндпоинты), например `python query.py meta`.

## Заметки

- Только чтение: инструменты делают лишь GET-запросы к `/v1/*`.
- Тот же ключ работает для OData-фида (Excel/Power BI) и для skill trade-data.
- Справочник: [`docs/api-reference.md`](../docs/api-reference.md).
