# Веб-сайт проекта (Quarto)

Сборка из **корня** репозитория:

```bash
quarto render site
```

Готовый сайт: `site/_site/index.html`.

## Зависимости R

Для страницы `bulletin.qmd` (интерактивные графики **highcharter**):

- `tidyverse` (или `readr`, `dplyr`)
- `forcats`
- `highcharter`

При использовании режима `params.data_source: "parquet"` дополнительно: `arrow`.

## Бюллетень: данные

1. **По умолчанию** — снимок [`data/bulletin_snapshot.csv`](data/bulletin_snapshot.csv): колонки `period`, `strana` (CN / IN / TR / ALL), `napr` (ЭК / ИМ), `idx`. Обновляйте файл при каждом выпуске (или генерируйте скриптом из `data_processed/fizob_2.parquet`).

2. **Из `fizob_2.parquet`** — в начале [`bulletin.qmd`](bulletin.qmd) задайте:

   ```yaml
   params:
     data_source: "parquet"
     fizob2_path: "../data_processed/fizob_2.parquet"
   ```

   Индекс на графике — среднее `fizob2` по всем 2-значным группам ТН ВЭД для каждой пары (страна, направление, месяц).

## Ежемесячный цикл

1. Обновить текст комментария в `bulletin.qmd`.
2. Обновить `data/bulletin_snapshot.csv` (или пересобрать `fizob_2.parquet` и рендерить с `data_source: "parquet"`).
3. Выполнить `quarto render site`.
