# Сборка статьи

Рабочая папка: `data_analytics/forecast_analysis/paper`.  
Основной файл: `nowcast_quality.tex`. Библиография: `sources.bib` (biblatex + biber). Рисунки: `figures/` (в tex подключены `.pdf`, для Word нужны `.png`).

Качественный текст, формулы и ссылки даёт **PDF через TeX Live**. Pandoc нужен только для **Word**. Не конвертируйте PDF→docx: формулы и вёрстка разъедутся.

## 1. PDF (TeXstudio / командная строка)

В TeXstudio: движок **pdflatex**, библиография **biber**. Сборка: `F5` / Build & View. Если ссылки `[?]` — соберите ещё раз (pdflatex → biber → pdflatex ×2).

Из PowerShell, из этой папки:

```powershell
cd "D:\Работа\mgimo-foreign-trade\data_analytics\forecast_analysis\paper"

pdflatex -interaction=nonstopmode nowcast_quality.tex
biber nowcast_quality
pdflatex -interaction=nonstopmode nowcast_quality.tex
pdflatex -interaction=nonstopmode nowcast_quality.tex
```

Результат: `nowcast_quality.pdf`.

Если правили только текст без `\cite` / `\ref`, достаточно одного `pdflatex`. Если меняли `sources.bib` или цитаты — полная цепочка с biber.

## 2. Word через pandoc

Word плохо глотает pdf-картинки. Перед pandoc подменяем расширение рисунков на png (сами `.png` уже лежат в `figures/`, dpi 400).

В PowerShell **кавычки вокруг `--resource-path` обязательны**: иначе `;` воспринимается как конец команды.

```powershell
cd "D:\Работа\mgimo-foreign-trade\data_analytics\forecast_analysis\paper"

$tex = Get-Content -Raw -Encoding UTF8 "nowcast_quality.tex"
$tex = $tex -replace 'fig_(\w+)\.pdf', 'fig_$1.png'
[System.IO.File]::WriteAllText("$pwd\nowcast_quality_docx.tex", $tex, [Text.UTF8Encoding]::new($false))

pandoc nowcast_quality_docx.tex `
  -o nowcast_quality.docx `
  --bibliography=sources.bib `
  --citeproc `
  --resource-path=".;figures" `
  -f latex `
  -t docx

Remove-Item nowcast_quality_docx.tex
```

Результат: `nowcast_quality.docx` с формулами, цитатами и четырьмя рисунками.

Проверка, что картинки внутри docx (не ссылки наружу):

```powershell
Copy-Item nowcast_quality.docx nowcast_quality.zip -Force
Expand-Archive nowcast_quality.zip -DestinationPath "$env:TEMP\docx_check" -Force
Get-ChildItem "$env:TEMP\docx_check\word\media"
Remove-Item nowcast_quality.zip, "$env:TEMP\docx_check" -Recurse -Force
```

Должны быть четыре `*.png` (~140–240 КБ каждый). Если папка `media` пустая — pandoc не нашёл рисунки: смотрите `--resource-path` и что в tex стоят `.png`, не `.pdf`.

### Что не делать

- `pandoc nowcast_quality.pdf -o nowcast_quality.docx` — плохие формулы и вёрстка.
- `pandoc nowcast_quality.tex ... --resource-path=.;figures` без кавычек в PowerShell — вторая часть команды не выполнится.
- Собирать Word из `.tex` с `\includegraphics{...pdf}` — картинки часто не вставляются.

## 3. Пересборка рисунков

Если меняли `results/results_table.xlsx` или код графиков:

```powershell
& "C:\Program Files\R\R-4.5.1\bin\Rscript.exe" `
  "D:\Работа\mgimo-foreign-trade\data_analytics\forecast_analysis\paper\figures\make_figures.R"
```

Пишет `fig_*.png` (dpi 400) и `fig_*.pdf` в `figures/`. После этого снова PDF и/или Word.

В статье сейчас используются: `fig_wmae`, `fig_relative`, `fig_time`, `fig_ranks`. Файл `fig_mae` собирается скриптом, но в tex не подключён.

## 4. Типичные сбои

| Симптом | Что сделать |
|---|---|
| В PDF `[?]`, пустой список литературы | `biber nowcast_quality`, затем два раза `pdflatex` |
| Biber ругается на `.bib` | кодировка UTF-8 без BOM; ключи `\cite{...}` совпадают с `@article{...}` |
| Pandoc: `resource not found` / нет рисунков в docx | кавычки у `--resource-path=".;figures"`, в промежуточном tex — `.png` |
| Формулы в Word «ломаются» | это ограничение pandoc; эталон — PDF. В Word правьте только прозу |

После правок `nowcast_quality.tex` сначала смотрите PDF, Word собирайте в конце.
