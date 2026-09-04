# API внешней торговли — справочник

Программный доступ к данным внешней торговли (и физобъёмам — позже). Тонкий
read-only сервис поверх DuckDB. Планы и решения — [api-plan.md](api-plan.md) и
[api-mvp-phase1.md](api-mvp-phase1.md); код — [`../api/`](../api/). Семантика полей —
[data_model.md](data_model.md).

> Статус: **развёрнуто на VPS (пилот)**. Работают `/health`, `/v1/meta`, `/v1/reference/*`,
> `/v1/trade`, `/v1/fizob` и **OData-фид** (`/odata/*`); токены выдаёт **кабинет в Superset**.
> Дальше — коммерческие тарифы и биллинг.

## База и версии

- **Пилот:** `http://217.26.28.186:8090` (HTTP, без TLS — токен идёт открыто; ограничить
  доступ фаерволом/туннелем до появления домена). Локально (dev): `http://localhost:8000`.
- Содержательные эндпоинты — под префиксом `/v1`; OData-фид — под `/odata`.
- **Интерактивная документация (OpenAPI/Swagger):** `GET /docs`, схема — `GET /openapi.json`.
- **Для аналитиков (Excel, без кода):** отдельная инструкция —
  [api-excel-guide.md](api-excel-guide.md).

## Авторизация

Каждый запрос (кроме `/health`) требует **персональный токен**, переданный **в заголовке**
(в query-параметре токен не принимается):

- `Authorization: Bearer <токен>` — для скриптов/интеграций.
- **HTTP Basic** (для Excel Power Query): имя пользователя — любое, **пароль = токен**.

Токен пользователь получает **сам** в кабинете Superset: страница
`http://217.26.28.186:8088/apikey/` (нужна роль `API`, назначает админ) → «Выпустить токен».
Токен показывается один раз; храните его. (Бутстрап через админа — `api/scripts/create_token.py`.)

## Эндпоинты

### `GET /health`
Проверка живости и версии данных. Без авторизации.
```json
{"status":"ok","rows":7416603,"period_min":"2019-01-01","period_max":"2026-06-01","data_version":"2026-06"}
```

### `GET /v1/meta`
Доступные измерения/меры/поля и ваш план/использование.
```json
{
  "data": {"rows":7416603,"period_min":"2019-01-01","period_max":"2026-06-01","data_version":"2026-06"},
  "filters": ["edizm","edizm_iso","napr","source","strana","tnved","tnved2","tnved4","tnved6","type","period_from","period_to"],
  "group_by": ["edizm","edizm_iso","napr","period","source","strana","tnved","tnved2","tnved4","tnved6","type","year"],
  "metrics": ["kol","netto","stoim"],
  "default_metrics": ["stoim","netto"],
  "include": ["country_name","tnved2_name","tnved4_name","tnved6_name","tnved_name","tnved_name_official","tnved_name_official_level","tnved_name_en","tnved_unit","tnved_name_source","tnved_translated"],
  "plan": {"code":"pilot","max_rows":1000000,"monthly_quota":null,"rate_limit_per_min":120},
  "usage": {"requests_this_month": 42},
  "limits": {"monthly_quota":null,"rate_limit_per_min":120,"requests_this_month":42,"remaining":null}
}
```
Блок `limits` показывает лимиты вашего плана и остаток (`remaining` = `monthly_quota − requests_this_month`; `null` — если квота без лимита).

### `GET /v1/reference/countries`
Справочник стран для фильтров/выпадашек: `[{"strana":"CN","country_name":"Китай"}, ...]`.

### `GET /v1/reference/tnved?level=2|4|6|8|10`
Коды и названия ТНВЭД заданного уровня:
`[{"code":"27","name":"Топливо минеральное...","name_en":null,"unit":null,"name_source":"fts"}, ...]`.
`unit` — дополнительная единица измерения, `name_source` — происхождение названия
(`fts` / `fns` — официальные справочники, `mt` — машинный перевод).

### `GET /v1/trade` — основной эндпоинт

Фильтрация и агрегация торговых данных. Все параметры опциональны.

| Параметр | Пример | Смысл |
|---|---|---|
| `strana` | `CN` (повторяемый) | страна-отчёт (ISO) |
| `napr` | `im` / `ex` (или `ИМ`/`ЭК`) | направление; ASCII-алиасы — основной путь |
| `type` | `fact` / `pred` | факт vs прогноз (nowcast) |
| `source` | `national`/`comtrade`/`nowcast` | источник |
| `tnved2` / `tnved4` / `tnved6` / `tnved` | `27` | фильтр по уровню кода |
| `edizm_iso` | `166` (повторяемый) | доп. единица по ISO-коду — **основной** фильтр единиц |
| `edizm` | `ШТУКА` | доп. единица по названию (кириллица в URL менее удобна) |
| `period_from`, `period_to` | `2024-01` | диапазон периодов (`YYYY-MM` или `YYYY-MM-DD`) |
| `group_by` | `strana,tnved2,period` | измерения агрегации (через запятую) |
| `metrics` | `stoim,netto,kol` | суммируемые меры; **по умолчанию `stoim,netto`** |
| `include` | `tnved4_name,country_name` | доп. поля-названия (opt-in) |
| `format` | `json` (по умолч.) / `csv` | формат ответа |
| `limit` | `10000` | размер страницы (`≤ plan.max_rows`) |
| `offset` | `0` | смещение (для keyset-курсора — следующая фаза) |
| `order_by` | `period` | сортировка (dim или мера в агрегации) |

**Два режима:**
- **Агрегация** — если задан `group_by`: возвращаются измерения + суммы мер.
- **Сырьё** — если `group_by` не задан: строки как есть (коды + меры + `PERIOD`),
  до `plan.max_rows` на страницу.

**Единицы и `KOL`.** Фильтруйте единицы по `edizm_iso`; `edizm`/`edizm_iso` доступны и в
ответе — без них `KOL` (количество в доп. единице) не интерпретируется. Мера **`kol`
аддитивна только внутри одной единицы**, поэтому в агрегации при `metrics=...,kol`
параметр **`edizm` обязан быть в `group_by`** — иначе `400`. `stoim` (USD) и `netto` (кг)
аддитивны без ограничений.

**Названия (`include`).** По умолчанию ответ «лёгкий» (только коды). `include` подтягивает
названия; в агрегации название требует соответствующий код в `group_by`
(напр. `tnved4_name` → `tnved4`).

Рядом с названием доступны `tnved_unit` (дополнительная единица измерения, отделённая
от текста названия) и `tnved_name_source`. Последний важен для интерпретации: `fts`/`fns` —
официальное русское наименование, `manual` — выверенное вручную, `mt` — машинный перевод
названия из зарубежного источника, такие подписи не следует цитировать как официальные.

Для машинных названий есть две опоры. `tnved_name_official` — ближайшее наименование из
официального справочника (для кода с машинной подписью на 10 знаках это будет официальный
текст на 8, 6 или 4 знаках); `tnved_name_official_level` говорит, на каком уровне оно нашлось,
то есть насколько огрубили. `tnved_name_en` — английский оригинал, из которого сделан перевод.
В OData-фиде доступны `TNVED_NAME_OFFICIAL` и `TNVED_NAME_EN`; уровень — только в REST.

**Пример.**
```
GET /v1/trade?strana=CN&napr=im&period_from=2024-01
    &group_by=tnved2,edizm,period&metrics=stoim,netto,kol&include=tnved2_name
```
```json
{
  "meta": {"rows": 240, "has_more": false, "next_offset": null,
           "page_rows": 10000, "max_rows": 1000000, "table": "unified_trade_data_enriched"},
  "data": [
    {"tnved2":"85","edizm":"ШТУКА","period":"2024-01-01",
     "tnved2_name":"Электрические машины...","stoim":1234.5,"netto":67.8,"kol":9.0}
  ]
}
```

### `GET /v1/fizob` — индексы физических объёмов

Индекс физического объёма (`fizob`) — база-нормированный показатель на уровне
(`STRANA`, `NAPR`, `tn_level`, `tn_code`, `PERIOD`). Агрегации нет — фильтр и постранично.

| Параметр | Пример | Смысл |
|---|---|---|
| `strana` | `CN` (повторяемый) | страна (ISO); `ALL` — сводный уровень |
| `napr` | `im` / `ex` | направление |
| `tn_level` | `2` (повторяемый) | уровень кода: `0` (страновой итог), `2`, `4`, `6` |
| `tn_code` | `27` (повторяемый) | код на этом уровне |
| `period_from`, `period_to` | `2024-01` | диапазон периодов |
| `order_by` / `format` / `limit` / `offset` | | как у `/v1/trade` |

Возвращает `STRANA, NAPR, PERIOD, tn_level, tn_code, tn_name, fizob, fizob_bp, idx`:
`idx` — сам индекс, `fizob_bp` — значение базового периода, `tn_name` — название для уровней
2/4. Пагинация — по `meta.has_more`/`next_offset`. Методология индексов — см.
[data_model.md](data_model.md) / техническую документацию.

Пример: `GET /v1/fizob?strana=CN&napr=im&tn_level=4&period_from=2024-01`

## Формат ответа и ошибки

- Успех: `{"meta": {...}, "data": [...]}`. `meta.has_more` = есть следующая страница
  (тогда `next_offset` — смещение для следующего запроса).
- Ошибки — `application/problem+json` (RFC 7807):
  `{"type":"about:blank","title":"...","status":400,"detail":"..."}`.
- `401` — нет/неверный токен; `400` — некорректные параметры (напр. `kol` без `edizm`).
- `429` — превышен лимит плана (частота или месячная квота); ответ содержит заголовок
  `Retry-After` (секунды до следующей попытки). См. раздел «Лимиты и тарифы».

## Лимиты и тарифы

У каждого пользователя есть **план** (`plan` в `/v1/meta`) с тремя ручками; enforcement
включён (`NULL` = без лимита):

| Ручка | Что ограничивает | Нарушение |
|---|---|---|
| `max_rows` | размер одной страницы (`limit ≤ max_rows`) | `limit` молча урезается до `max_rows` |
| `rate_limit_per_min` | запросов в минуту | `429` + `Retry-After: 60` |
| `monthly_quota` | запросов за календарный месяц | `429` + `Retry-After: 3600` |

Текущие тарифы (в `api.plans`, меняются данными без деплоя):

| План | rate/мин | квота/мес | max_rows |
|---|---|---|---|
| `pilot` | 120 | ∞ (NULL) | 1 000 000 |
| `free` | 30 | 5 000 | 50 000 |
| `pro` | 120 | 100 000 | 1 000 000 |

Остаток месячной квоты виден в `/v1/meta` → `limits.remaining`. Счётчики — в Redis; при
недоступности Redis лимиты **не применяются** (fail-open, сервис не блокируется). Полная
выгрузка больших объёмов — постранично (`limit` + `offset`; keyset-курсор — в планах),
суммарный объём упирается в месячную квоту плана.

## Excel (Power Query) — пошагово

1. **Данные → Получить данные → Из интернета**.
2. URL c нужными фильтрами, напр.:
   `https://<домен>/v1/trade?strana=CN&napr=im&period_from=2024-01&group_by=tnved2,period&format=json`
3. Тип доступа — **Основной (Basic)**: имя — любое, **пароль — ваш токен**.
4. Power Query развернёт JSON в таблицу (типы и UTF-8 сохранены — без проблем с
   разделителями). Обновление — кнопкой «Обновить».

> Рекомендуется **JSON**, а не CSV: в русской локали Excel CSV страдает от разделителей и
> кодировок. CSV (`format=csv`) отдаётся с UTF-8 BOM и `;`, числа — с точкой.

## OData-фид (Excel / Power BI)

Нативный доступ для BI без формул — «Из веб-канала OData»:
- Сервис: `GET /odata/` · схема: `GET /odata/$metadata` · данные: `GET /odata/trade`.
- Сущность `trade` = строки торговли **с названиями ТНВЭД** (`TNVED_NAME`, `TNVED2_NAME`,
  `TNVED4_NAME` рядом с кодами; страна — кодом). Поддержаны `$filter`, `$select`, `$orderby`,
  `$top`, `$skip`, `$count`, серверная пагинация (`@odata.nextLink`).
- Авторизация — тот же токен (Basic: имя любое, пароль = токен).

**Excel:** Данные → Получить данные → **Из веб-канала OData** → URL `http://<host>:8090/odata/`
→ Basic-auth (токен) → выбрать `trade` → фильтровать мышкой в редакторе (фолдится в `$filter`) →
загрузить. Обновление — «Обновить всё».

**Поддержка `$filter`** (подмножество OData v4): сравнения `eq`, `ne`, `gt`, `ge`, `lt`, `le`;
логические `and`, `or`; скобки. Значения — строки в одинарных кавычках (`'CN'`), числа,
даты (`2024-01-01`). Поля — из схемы сущности (`STRANA`, `PERIOD`, `NAPR`, `TNVED*`, `STOIM`
и т.д.). Функции (`contains`, `startswith`) пока не поддержаны. Другие опции: `$select`
(колонки), `$orderby` (`PERIOD desc`), `$top`/`$skip` (страница), `$count=true` (счётчик).

Пример прямого запроса:
```
GET /odata/trade?$filter=STRANA eq 'CN' and PERIOD ge 2024-01-01&$select=PERIOD,TNVED2,STOIM&$top=1000
```

## Доступ из Python и R

Для скриптов используйте `/v1/trade` (JSON) с заголовком `Authorization: Bearer <токен>`.
Ниже — выгрузка с автоматической постраничной докачкой в таблицу. Токен — из кабинета;
базовый URL пилота — `http://217.26.28.186:8090`.

### Python (`requests` + `pandas`)
```python
import requests, pandas as pd

BASE, TOKEN = "http://217.26.28.186:8090", "mgt_ВАШ_ТОКЕН"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def fetch_trade(**params) -> pd.DataFrame:
    rows, offset = [], 0
    while True:
        r = requests.get(f"{BASE}/v1/trade", headers=HEADERS,
                         params={**params, "limit": 10000, "offset": offset})
        r.raise_for_status()
        body = r.json()
        rows += body["data"]
        if not body["meta"]["has_more"]:
            break
        offset = body["meta"]["next_offset"]
    return pd.DataFrame(rows)

df = fetch_trade(strana="CN", napr="im", period_from="2024-01",
                 group_by="tnved2,period", include="tnved2_name")
print(df.head())
# несколько стран: strana=["CN","DE"] (повторяемый параметр)
```

### R (`httr2` + `dplyr`)
```r
library(httr2); library(dplyr)

base <- "http://217.26.28.186:8090"; token <- "mgt_ВАШ_ТОКЕН"

fetch_trade <- function(...) {
  out <- list(); offset <- 0
  repeat {
    resp <- request(base) |> req_url_path("/v1/trade") |>
      req_url_query(..., limit = 10000, offset = offset) |>
      req_auth_bearer_token(token) |> req_perform() |>
      resp_body_json(simplifyVector = TRUE)
    out <- append(out, list(resp$data))
    if (!isTRUE(resp$meta$has_more)) break
    offset <- resp$meta$next_offset
  }
  bind_rows(out)
}

df <- fetch_trade(strana = "CN", napr = "im", period_from = "2024-01",
                  group_by = "tnved2,period", include = "tnved2_name")
head(df)
```

Примечания: параметры те же, что у [`/v1/trade`](#get-v1trade--основной-эндпоинт); без `group_by`
приходят сырые строки (тоже с пагинацией). Помните о лимитах плана (`429` при превышении
частоты/квоты — см. «Лимиты и тарифы»); при большом объёме фильтруйте по стране/периоду.
Нужен формат для выгрузки в файл — добавьте `format=csv` (UTF-8 BOM, разделитель `;`).

## Семантика данных

Значения полей (`STOIM` в USD, `NETTO` в кг, `KOL` в доп. единице, `NAPR`, `TYPE`,
`SOURCE`, уровни ТНВЭД) — см. [data_model.md](data_model.md).

## Дорожная карта

Готово и в проде: `/v1/trade`, `/v1/reference/*`, OData-фид, кабинет в Superset (роль-допуск),
аудит + админ-дашборд использования, enforcement квот/rate-limit.

Осталось (см. [api-plan.md](api-plan.md)): таймаут запроса; TLS + домен (сейчас HTTP);
коммерческие тарифы и биллинг.
