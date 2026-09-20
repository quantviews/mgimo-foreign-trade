# Лендинг на VPS (nginx, порт 80)

Как статический сайт (лендинг + бюллетень + презентации) публикуется на VPS
`http://217.26.28.186/` и как его обновлять с локального ПК.

## Зачем

Сайт собирается Quarto и по умолчанию публикуется на GitHub Pages
(`quantviews.github.io/mgimo-foreign-trade`). На VPS мы держим **ту же сборку**,
чтобы она была доступна независимо от GitHub (важно при блокировках/лагах) и
рядом с Superset (`:8088`) и API (`:8090`).

## Архитектура

- Сайт **статический**: готовые HTML/CSS/JS, никакой сборки на сервере.
- На VPS работает контейнер nginx `landing`, отдающий каталог
  `/home/marcel/mgimo-landing` на порту 80.
- **Сборка идёт на локальном ПК** (нужны Quarto + R + Python/jupyter и локальная
  DuckDB для бюллетеня), результат заливается на VPS скриптом.
- Ссылки навбара на презентации сделаны относительными (`../presentations/...`)
  в корневом `_quarto.yml`, поэтому сайт самодостаточен и на VPS, и на gh-pages.

## Разовая настройка VPS (уже выполнена)

Каталог под сайт (в домашней папке marcel, чтобы деплой шёл по scp без sudo),
конфиг nginx (лендинг + прокси на Superset) и контейнер nginx:

```bash
# конфиг nginx из репозитория -> на VPS (с локального ПК):
scp deploy/landing-nginx.conf mgimo:/home/marcel/landing-nginx.conf

ssh mgimo
mkdir -p /home/marcel/mgimo-landing
docker run -d --name landing --restart unless-stopped \
  --network superset_default \
  -p 80:80 \
  -v /home/marcel/mgimo-landing:/usr/share/nginx/html:ro \
  -v /home/marcel/landing-nginx.conf:/etc/nginx/conf.d/default.conf:ro \
  nginx:alpine
```

- `--network superset_default` — чтобы nginx достучался до Superset по имени
  сервиса (`superset-superset-1:8088`).
- монтируется [`deploy/landing-nginx.conf`](../deploy/landing-nginx.conf) —
  статический сайт на `/` и прокси Superset на `/superset` (см. ниже).

Проверка: `curl -s -o /dev/null -w '%{http_code}\n' http://localhost:80` даёт 200.
Порт 80 на VPS был свободен. Контейнер поднимается сам после перезагрузки
(`--restart unless-stopped`).

## Обновление (деплой с локального ПК)

Одной командой из корня репозитория:

```bash
bash scripts/deploy_landing_vps.sh
```

### Как запускать (Windows)

Запуск **ручной**, через **Git Bash** (не WSL). Два способа — выбери любой.

**Способ 1 — двойной клик (проще всего):**

1. Открой папку репозитория в Проводнике.
2. Дважды кликни по файлу **`deploy_landing.cmd`** в корне репозитория.
3. Откроется окно консоли, пойдёт сборка и заливка (несколько минут). В конце
   появится строка `Deployed. Open: http://217.26.28.186/`.
4. Нажми любую клавишу, чтобы закрыть окно.

Если Git установлен не на диске `H:`, открой `deploy_landing.cmd` блокнотом и
поправь путь в строке `set "GITBASH=..."` (там есть запасной вариант для `C:`).

**Способ 2 — вручную в Git Bash:**

1. Открой **Git Bash** (правый клик в папке репозитория → «Open Git Bash here»,
   либо из меню «Пуск»).
2. Если открылся не в папке проекта, перейди в неё:
   ```bash
   cd /f/Yandex.Disk/HSE/mgimo-foreign_trade
   ```
3. Запусти деплой:
   ```bash
   bash scripts/deploy_landing_vps.sh
   ```
4. Дождись строки `Deployed. Open: http://217.26.28.186/`.

Почему не WSL: скрипт использует виндовые quarto/R/python и ssh-хост `mgimo` из
виндового `~/.ssh/config`; в WSL всё это пришлось бы ставить и настраивать заново.

Что делает скрипт (зеркалит сборку из `.github/workflows/publish.yml`, но шлёт на
VPS, а не в gh-pages):

1. `quarto render .` — сайт, техдок, бюллетень, уроки → `_site/`.
2. `quarto render presentations -P inline_charts:true` — деки с общим `site_libs`
   и встроенными графиками → `presentations/_site/`.
3. копирует бандл презентаций в `_site/presentations/`.
4. пакует `_site` в tar, заливает по scp, распаковывает в
   `/home/marcel/mgimo-landing` на VPS.

После этого сайт доступен на `http://217.26.28.186/`.

### Что нужно на ПК

- `quarto`;
- R (`G:/R/R-4.5.1/bin`) — графики сайта/бюллетеня/дек;
- Python с `jupyter`+`pandas`+`matplotlib` (`H:/conda/envs/py312/python.exe`) —
  слайды уроков (`lessons/*.qmd` содержат `{python}` чанки);
- `ssh`/`scp`/`tar` и ssh-хост `mgimo` (в `~/.ssh/config`);
- свежие `db/unified_trade_data.duckdb` и снимки `site/data/*.parquet` для
  бюллетеня (снимки пересобираются `Rscript site/bulletin_data_prep.R`; полный
  цикл — в оркестрации пайплайна).

### Переменные окружения (переопределяют дефолты скрипта)

| Переменная | По умолчанию |
|---|---|
| `VPS_HOST` | `mgimo` |
| `VPS_LANDING_DIR` | `/home/marcel/mgimo-landing` |
| `R_BIN` | `/g/R/R-4.5.1/bin` |
| `QUARTO_PYTHON` | `H:/conda/envs/py312/python.exe` |

## Когда запускать

Деплой **ручной**: запускаешь его сам, когда нужно обновить сайт на VPS (как —
см. «Как запускать» выше). По расписанию не ставим. Обычно это делают после того,
как пересобрал данные/бюллетень и хочешь, чтобы обновление увидел сайт на VPS.

## Структура на сервере

- `/` → мгновенный редирект на `/site/index.html` (главная);
- `/superset/` — Superset (реверс-прокси на `:8088`, см. ниже);
- `/site/` — страницы сайта (главная, техдок, месячный бюллетень);
- `/presentations/` — деки (`project-overview`, `dataviz-story`);
- `/lessons/` — уроки.

## Superset под /superset

Superset (`:8088`, отдельный Docker-стек) доступен по `http://217.26.28.186/superset/`
через тот же контейнер `landing`. Прямой доступ по `:8088` при этом сохраняется.

Что настроено:

1. **nginx** ([`deploy/landing-nginx.conf`](../deploy/landing-nginx.conf)):
   `location /superset/` проксирует на `superset-superset-1:8088` и ставит заголовок
   `X-Forwarded-Prefix: /superset` (контейнер `landing` подключён к сети
   `superset_default`).
2. **superset_config.py** (`/home/marcel/superset/superset_config.py`):
   - `ENABLE_PROXY_FIX = True` — Superset учитывает X-Forwarded-* (было заранее),
     поэтому серверные ссылки/редиректы/логин получают префикс `/superset`;
   - `STATIC_ASSETS_PREFIX = "/superset"` — **добавлено**, чтобы фронтовые ассеты
     (webpack, CSS/JS) грузились с `/superset/static/...`, а не с голого `/static/`;
   - `APPLICATION_ROOT = "/superset"` — **добавлено**, иначе React-фронтенд считает
     базой `/` и зовёт API по голому `/api/...` (404) → страница логина остаётся
     пустой. Superset берёт `application_root` для фронта из этого конфига
     (`superset/views/base.py`), а не из ProxyFix; маршрутизацию делает SCRIPT_NAME,
     так что дублирования префикса нет.
   После правки конфига Superset перезапускается:
   `docker restart superset-superset-1 superset-worker-1 superset-beat-1`.
3. **nginx-редирект одинарного префикса на двойной** (там же в conf):
   у Superset есть собственные маршруты под `/superset/*` (`welcome`, `explore`,
   `sqllab`, `profile`), поэтому под нашим внешним `/superset` их корректный
   внешний адрес — **двойной** `/superset/superset/...`. Корень
   (`/superset/`) Superset редиректит на двойной адрес правильно, но FAB после
   логина строит `next` из уже очищенного от префикса PATH_INFO и теряет один
   сегмент — даёт одинарный `/superset/welcome/`, который после среза префикса
   в nginx превращается в `/welcome/` и ловит 404. Поэтому в conf есть
   `location ~ ^/superset/(welcome|explore|sqllab|profile)` с
   `return 301 /superset/superset/$1$2` — он возвращает недостающий сегмент.
   Корневые маршруты (`login`, `api`, `static`, `health`, `dashboard/list`,
   `chart/list`) под этот редирект не попадают и работают как есть.

4. **Логотип (`APP_ICON`)** — тот же, что в hero лендинга
   (`site/assets/mgimo-wordmark.svg`). Задан **полным URL**
   `http://nts.mgimo.ru/site/assets/mgimo-wordmark.svg`, потому что один и тот же
   `APP_ICON` отдаётся и на прямом `:8088` (там нужен голый `/static/...`), и под
   `/superset` (там нужен `/superset/static/...`) — относительным путём оба входа
   не покрыть, а абсолютный URL резолвится отовсюду и всегда совпадает с
   лендингом. `LOGO_RIGHT_TEXT` убран: в wordmark уже есть надпись «МГИМО».
   Если появится HTTPS — заменить `http://` на `https://` в этом URL.

   **Важно про адресную строку:** внутренние страницы Superset открываются по
   двойному пути (`.../superset/superset/welcome/`, дашборды тоже). Это рабочее
   состояние: весь фронт (basename, API, ассеты) консистентен на двойном
   префиксе. Так вышло из-за совпадения нашего подпутя `/superset` с внутренним
   сегментом маршрутов самого Superset. Если двойной путь в URL мешает —
   чистое решение это поддомен (например `bi.nts.mgimo.ru`) вместо подпутя.

Обновить конфиг nginx (после правки `deploy/landing-nginx.conf`):

```bash
scp deploy/landing-nginx.conf mgimo:/home/marcel/landing-nginx.conf
ssh mgimo "docker exec landing nginx -t && docker exec landing nginx -s reload"
```

Проверка подпути:
- ассеты идут с `/superset/static/...` (не с голого `/static/`):
  `curl -sL http://217.26.28.186/superset/ | grep -oE '/static/[^"]+' | head`
  (если видишь голый `/static/` — не применился `STATIC_ASSETS_PREFIX` или не было
  рестарта Superset);
- API фронтенда доходит до Superset (401, а не 404):
  `curl -o /dev/null -w '%{http_code}\n' http://217.26.28.186/superset/api/v1/me/`.

## Диагностика

- **Сайт не открывается:** `ssh mgimo docker ps | grep landing` (контейнер up?),
  `ssh mgimo docker logs landing`.
- **403 / пусто:** каталог `/home/marcel/mgimo-landing` пуст — прогнать деплой.
- **Презентации не открываются:** проверить, что в `_site/presentations/` есть
  файлы (шаг 3 деплоя копирует бандл презентаций).
- **Порт 80 занят:** `ssh mgimo "sudo ss -ltnp | grep :80"` (нужен sudo).
- **Правка контейнера:** пересоздать — `ssh mgimo docker rm -f landing` и команда
  `docker run ...` из раздела настройки.
