# Публикация сайта

Как собрать и опубликовать сайт проекта (лендинг, техническая документация,
ежемесячный бюллетень и занятия) на GitHub Pages.

## Коротко

- Публикация **автоматическая**: push в `main` с изменениями в `site/**`,
  `lessons/**`, `other/**`, `_quarto.yml` или `site/_quarto.yml` запускает
  GitHub Actions, который собирает сайт и кладёт его на ветку `gh-pages`.
- Живой сайт: **<https://quantviews.github.io/mgimo-foreign-trade/site/index.html>**
  (главная сейчас лежит под `/site/` — см. «Известные нюансы»).
- Перед публикацией нового месяца нужно **пересобрать снапшоты данных
  бюллетеня** (`site/data/*.parquet`), иначе сборка упадёт.

## Что и откуда собирается

Деплой собирается из **корневого [`_quarto.yml`](../_quarto.yml)** командой
`quarto render .`. Он рендерит:

- `site/index.qmd` — лендинг (главная),
- `site/technical.qmd` — «Как это работает»,
- `site/bulletin_comparison.qmd` — ежемесячный бюллетень (нужны данные, см. ниже),
- `other/*.qmd` — вспомогательные страницы/слайды,
- `lessons/lesson_01…04.qmd` — занятия курса (revealjs-слайды).

Результат складывается в `_site/` и публикуется на `gh-pages`.

> **Важно про два конфига.** В репозитории два файла Quarto-проекта:
> - **`_quarto.yml` (корень)** — единственный, из которого идёт деплой.
>   Навбар, логотип, пункты меню и тема на живом сайте берутся **отсюда**.
> - **`site/_quarto.yml`** — только для локального превью `site/` (3 страницы).
>
> Если меняете навбар/логотип — **правьте оба файла**, иначе изменения не
> попадут на живой сайт (правки только в `site/_quarto.yml` видны лишь локально).
> Стили (`site/styles/mgimo.scss`) общие — на них ссылаются оба конфига.
>
> Ещё есть `lessons/_quarto.yml` — отдельный revealjs-проект для локального
> рендера занятий; на деплое занятия собирает корневой проект.

## Данные бюллетеня (обязательный шаг перед выпуском)

Страница бюллетеня читает готовые снапшоты из `site/data/`:

- `bulletin_fo.parquet`, `tab_stoim_oil.parquet`, `df_groups.parquet`,
  `data_oilgas.parquet`, `hs4_labels.parquet`.

Они **лежат в git** (в `.gitignore` для них сделано исключение) и их нужно
пересобирать при обновлении базы. Генерация — скриптом
[`site/bulletin_data_prep.R`](../site/bulletin_data_prep.R):

```bash
# из корня репозитория; нужен db/unified_trade_data.duckdb
Rscript site/bulletin_data_prep.R
```

Скрипт берёт базу из `MGIMO_DUCKDB_PATH` или из `db/unified_trade_data.duckdb`.
Нужны R-пакеты: `tidyverse`, `duckdb`, `arrow`, `forecast`, `jsonlite`.

CI **проверяет наличие** всех пяти снапшотов и падает, если хоть одного нет
(с подсказкой запустить `bulletin_data_prep.R`). Поэтому свежие
`site/data/*.parquet` нужно закоммитить вместе с изменениями бюллетеня.

## Обычный цикл выпуска (ежемесячно)

1. **Обновить базу** `db/unified_trade_data.duckdb` — см.
   [orchestration.md](orchestration.md).
2. **Пересобрать снапшоты бюллетеня:** `Rscript site/bulletin_data_prep.R`.
3. **Обновить тексты** бюллетеня/лендинга при необходимости.
4. **Проверить локально** (см. «Локальная проверка»).
5. **Закоммитить и запушить в `main`** — включая обновлённые `site/data/*.parquet`.
6. CI соберёт и опубликует сайт. При желании — проследить статус:
   `gh run watch` или вкладка **Actions → Quarto Publish**.

## Локальная проверка

- **Быстрый превью лендинга/техдоков** (без бюллетеня, использует `site/_quarto.yml`):

  ```bash
  quarto render site/index.qmd
  quarto render site/technical.qmd
  # результат: site/_site/*.html
  ```

- **Полная сборка как на деплое** (из корня, использует `_quarto.yml`):

  ```bash
  quarto render .
  # результат: _site/**; тяжело — нужен весь R + Python стек
  ```

  Полная сборка требует R-пакетов для бюллетеня и Python (`jupyter`,
  `nbformat`, `ipykernel`) для занятий. Обычно достаточно превью отдельных
  страниц, а полную сборку доверить CI.

- **Занятия (revealjs):** `quarto render lessons`.

## Как устроен CI

Workflow: [`.github/workflows/publish.yml`](../.github/workflows/publish.yml).

- **Триггеры:** push в `main` по путям `site/**/*.qmd|*.R|styles/**`,
  `site/data/**`, `site/figures/**`, `other/**/*.qmd`, `lessons/**/*.qmd|*.css`,
  `_quarto.yml`, `site/_quarto.yml`, самого workflow; плюс ручной запуск
  (`workflow_dispatch`).
- **Шаги:** установка Quarto (закреплена версия `1.7.34`), R + пакеты (`pak`),
  Python + пакеты для занятий, системные библиотеки для шрифтов, **проверка
  снапшотов** `site/data/*.parquet`, `quarto render .`, публикация `_site` на
  `gh-pages` (`peaceiris/actions-gh-pages`).
- **Права:** `contents: write` (пуш в `gh-pages`).

## Ручная публикация

- **Через GitHub:** вкладка **Actions → Quarto Publish → Run workflow**
  (`workflow_dispatch`).
- **Локально** (если очень нужно): собрать `quarto render .` и опубликовать
  `_site/` на `gh-pages`. Штатный путь — CI; ручную публикацию используйте как
  резервную.

## Известные нюансы

- **Главная под `/site/`.** Корневой проект рендерит страницы с сохранением
  папки `site/`, поэтому на `gh-pages` нет `index.html` в корне: базовый URL
  `…/mgimo-foreign-trade/` отдаёт 404, а вход — `…/mgimo-foreign-trade/site/index.html`.
  *Улучшение:* добавить в корень редирект (`index.qmd`/`index.html`,
  перенаправляющий на `site/index.html`), чтобы главная открывалась с базового URL.
- **Навбар/логотип — только из корневого `_quarto.yml`.** При правках держите
  оба конфига в синхроне (см. врезку выше).
- **Занятия** на живом сайте — по пути `…/lessons/lesson_0X.html`. В локальном
  превью `site/` их нет (собираются корневым проектом), поэтому ссылки в
  `site/_quarto.yml` ведут на опубликованные слайды.
- **Версия Quarto.** CI закреплён на `1.7.34`; локально может стоять новее —
  при расхождении рендера ориентируйтесь на версию CI.

## Ссылки

- [orchestration.md](orchestration.md) — обновление базы `unified_trade_data.duckdb`.
- [site/README.md](../site/README.md) — сборка сайта и данные бюллетеня.
- [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) — сам пайплайн публикации.
