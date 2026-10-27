Техническая документация для команды, которая собирает данные, развивает сервисы и публикует обновления.

Для работы с готовыми данными используйте [пользовательскую справку API](https://nts.mgimo.ru/site/api.html), [инструкцию Excel](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/api-excel-guide.md) или [справку MCP](https://nts.mgimo.ru/site/mcp.html).

## Начало работы

Установите зависимости и пакет проекта из корня репозитория:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
pytest -q
```

Исходные выгрузки, база DuckDB и ключи доступа не поставляются вместе с кодом. Для пересборки сначала подготовьте источники и локальное окружение по инструкциям ниже.

## Сбор и обновление данных

- [Полный цикл через Prefect](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/orchestration.md): порядок refresh, повторная сборка после nowcast и fizob, проверки качества.
- [Сборка единой DuckDB](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/merge_processed_data-docs.md): merge pipeline и параметры CLI.
- [Общий контракт сборщиков](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/country-collector-guide.md): правила добавления источников.
- [Китай: оригинальные выгрузки и подтверждение полноты](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/china-original-exports-2026-10-24.md).
- [Индия: сбор данных](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/india-collector-docs.md) и [обработка](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/india-processor-docs.md).
- [Турция: сбор данных](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/turkey-collector-docs.md).
- [UN Comtrade: загрузка и пересмотры](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/comtrade-collector-docs.md), [сборка отдельной базы](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/merge-comtrade-to-duckdb-docs.md).
- [Покрытие публикаций и отсутствующие строки](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/source-absence-rules-2026-10-24.md): когда отсутствие записи нельзя считать нулём.

## Модель данных и расчёты

- [Модель данных](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/data_model.md): поля, источники, факт и nowcast, справочники, схема fizob.
- [Проверка новой методологии Фишера](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/fizob-fisher-audit-2026-10-24.md): пропуски, нули, агрегаты и тесты.
- [Разбор покрытия](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/fisher-coverage-blockers-2026-10-24.md) и [эксперимент с сопоставимой корзиной](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/fisher-matched-basket-2026-10-24.md). Эксперимент не заменяет опубликованный полный индекс.
- [Ранее описанный расчёт fizob](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/documentation_fizob.md): читать вместе с текущей схемой и датированными изменениями выше.

Неизвестные значения не заменяем нулями. Полные индексы с неподтверждённым покрытием не публикуем. Факт, оценки источников и nowcast проекта сохраняем раздельно.

## API, MCP и демо-чат

- [Справочник API](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/api-reference.md): эндпоинты, параметры и OData.
- [Развёртывание API](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/api-deploy.md).
- [Код и инструкции API](https://github.com/quantviews/mgimo-foreign-trade/blob/main/api/README.md), [MCP-сервера](https://github.com/quantviews/mgimo-foreign-trade/blob/main/mcp-server/README.md) и [демо-чата](https://github.com/quantviews/mgimo-foreign-trade/blob/main/demo/README.md).
- [Первоначальный план API](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/api-plan.md) и [спецификация первого этапа](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/api-mvp-phase1.md) - проектные материалы, а не инструкция по текущему интерфейсу.

Для локальной разработки API:

```bash
python -m pip install -r api/requirements.txt
uvicorn app.main:app --app-dir api --reload
```

Авторизация и переменные окружения описаны в инструкции сервиса. Не сохраняйте рабочие ключи в репозитории.

## Тесты и публикация

- [Запуск тестов](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/testing-docs.md).
- [Обновление бюллетеней](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/bulletin-refresh.md): подготовка снимков, сверка чисел, HTML/PDF и публикация.
- [Публикация сайта](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/site-publishing.md) и [размещение на VPS](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/vps-landing.md).
- [Витрина fizob в Superset](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/superset-fizob.md).
- [Сентябрьский релиз 2026](https://github.com/quantviews/mgimo-foreign-trade/blob/main/docs/september-release-2026-10-24.md): состав обновления, проверки и откат.

Обработку исходных данных, nowcast, fizob и пересборку DuckDB выполняем локально. На VPS передаём готовые проверенные результаты; перед заменой сохраняем резервную копию и после публикации проверяем сервисы.

## Как поддерживать документацию

Технические инструкции хранятся в `docs/`. Этот указатель включается в страницу Quarto без копирования текста. При изменении поведения обновляйте соответствующую инструкцию; датированные аудиты сохраняйте как историю проверок и явно отделяйте эксперименты от опубликованного решения.
