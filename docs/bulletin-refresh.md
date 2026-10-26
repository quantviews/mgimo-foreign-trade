# Обновление текущих бюллетеней

Выполнять локально после проверки новой DuckDB. На VPS передавать только
готовые страницы, PDF и ресурсы; заранее сохранять предыдущую версию.

## Подготовка

На Windows R должен работать в UTF-8. Linux-значение `LC_ALL=C.UTF-8`
не поддерживается установленной Windows-сборкой R и может испортить
кириллические фильтры. Перед запуском в PowerShell:

```powershell
$env:LC_ALL = 'English_United States.utf8'
$env:LC_CTYPE = $env:LC_ALL
$env:LANG = $env:LC_ALL
$env:QUARTO_R = 'G:/R/R-4.5.1/bin'
$env:PATH = $env:QUARTO_R + ';' + $env:PATH
& 'G:/R/R-4.5.1/bin/Rscript.exe' site/bulletin_data_prep.R
foreach ($country in @('china', 'india', 'turkey')) {
  & 'G:/R/R-4.5.1/bin/Rscript.exe' "site/country_bulletins/$country/data_prep_$country.R"
}
& 'H:/conda/envs/py312/python.exe' scripts/validate_bulletin_snapshots.py
```

Проверка сверяет все месячные суммы, фактическую стоимость и массу нефтяного
кода с DuckDB, а fizob - с общей витриной. Пустые сводные снимки запрещены.
Индексы с несовместимыми базами или неизвестными значениями не публикуются.
Общий индекс берётся из ALL. Страновые индексы не усредняются.

Стоимость включает доступные факты и nowcast; доля оценок рассчитывается
для последнего месяца. Изменения товарных групп относятся к доступной стоимости
двух явно указанных годовых окон и не подтверждают одинаковый охват публикаций.
Масса переводится из кг в млн т делением на 1e9; неизвестная масса остаётся NULL.
Турецкие нераскрытые позиции 2799 не переклассифицируются в сырую нефть.

## Рендер и проверка

```powershell
& 'D:/Program Files/Quarto/bin/quarto.exe' render site/bulletin_comparison.qmd --to html
foreach ($country in @('china', 'india', 'turkey')) {
  & 'D:/Program Files/Quarto/bin/quarto.exe' render "site/country_bulletins/$country/bulletin_${country}_pdf.qmd" --profile bulletins --to all
  Copy-Item -LiteralPath "site/_site/country_bulletins/$country/bulletin_${country}_pdf.pdf" -Destination "site/country_bulletins/$country/bulletin_${country}_pdf.pdf"
}
```

Выборочный рендер использует вложенный проект `site/_quarto.yml` и профиль
`site/_quarto-bulletins.yml`. Результат находится в **site/_site**, а не в
корневом `_site`. Профиль сохраняет общую навигацию на страновых страницах.
Проверить все страницы PDF визуально, дату выпуска, фактические значения,
подписи nowcast и единицы. Открыть четыре HTML-страницы в браузере; проверить
графики, ошибки JavaScript, HTTP-ошибки ресурсов и ссылки на PDF.

## Публикация

Скопировать из `site/_site` в каталог `site` публикации только:

- `bulletin_comparison.html` и `bulletin_comparison_files`;
- HTML, PDF и соответствующий каталог `_files` каждого странового бюллетеня;
- необходимые ресурсы `site_libs`.

Не заменять весь сайт старой локальной сборкой. Перед наложением архива
сохранить прежние файлы и ресурсы в резервный архив. После публикации сверить
SHA256 всех семи HTML/PDF-файлов через HTTP и проверить страницы браузером.
Откат: восстановить сохранённый архив в тот же корень сайта.
