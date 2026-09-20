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

Каталог под сайт (в домашней папке marcel, чтобы деплой шёл по scp без sudo) и
контейнер nginx:

```bash
ssh mgimo
mkdir -p /home/marcel/mgimo-landing
docker run -d --name landing --restart unless-stopped \
  -p 80:80 -v /home/marcel/mgimo-landing:/usr/share/nginx/html:ro nginx:alpine
```

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
- `/site/` — страницы сайта (главная, техдок, месячный бюллетень);
- `/presentations/` — деки (`project-overview`, `dataviz-story`);
- `/lessons/` — уроки.

## Диагностика

- **Сайт не открывается:** `ssh mgimo docker ps | grep landing` (контейнер up?),
  `ssh mgimo docker logs landing`.
- **403 / пусто:** каталог `/home/marcel/mgimo-landing` пуст — прогнать деплой.
- **Презентации не открываются:** проверить, что в `_site/presentations/` есть
  файлы (шаг 3 деплоя копирует бандл презентаций).
- **Порт 80 занят:** `ssh mgimo "sudo ss -ltnp | grep :80"` (нужен sudo).
- **Правка контейнера:** пересоздать — `ssh mgimo docker rm -f landing` и команда
  `docker run ...` из раздела настройки.
