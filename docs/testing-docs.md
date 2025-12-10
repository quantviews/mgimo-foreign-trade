# Документация по тестированию

## Обзор

Проект использует pytest для тестирования модулей. Тесты находятся в директории `tests/` и покрывают основные функции модулей обработки данных.

## Установка зависимостей

Для запуска тестов необходимо установить pytest и другие зависимости:

```bash
pip install pytest pandas duckdb
```

Или используйте файл requirements.txt, если он есть в проекте.

## Структура тестов

```
tests/
├── __init__.py
└── test_merge_processed_data.py
```

## Запуск тестов

### Запуск всех тестов

```bash
# Из корневой директории проекта
pytest tests/ -v
```

### Запуск конкретного тестового файла

```bash
pytest tests/test_merge_processed_data.py -v
```

### Запуск конкретного тестового класса

```bash
pytest tests/test_merge_processed_data.py::TestValidateSchema -v
```

### Запуск конкретного теста

```bash
pytest tests/test_merge_processed_data.py::TestValidateSchema::test_valid_schema -v
```

### Запуск с выводом print-ов

```bash
pytest tests/ -v -s
```

### Запуск с покрытием кода (если установлен pytest-cov)

```bash
pytest tests/ --cov=src --cov-report=html
```

## Тесты для merge_processed_data.py

### TestValidateSchema

Тестирует функцию `validate_schema`, которая проверяет соответствие DataFrame ожидаемой схеме данных.

**Тесты:**
- `test_valid_schema` - проверка валидной схемы
- `test_missing_columns` - проверка отсутствующих колонок
- `test_invalid_napr_values` - проверка невалидных значений NAPR
- `test_null_period` - проверка null значений в PERIOD
- `test_wrong_data_types` - проверка неверных типов данных

**Пример использования:**
```python
df = pd.DataFrame({
    'NAPR': ['ИМ', 'ЭК'],
    'PERIOD': pd.to_datetime(['2024-01-01', '2024-02-01']),
    # ... другие колонки
})
assert validate_schema(df, 'test.parquet') == True
```

### TestGenerateDerivedColumns

Тестирует функцию `generate_derived_columns`, которая генерирует производные колонки TNVED (TNVED2, TNVED4, TNVED6, TNVED8).

**Тесты:**
- `test_generate_tnved_columns` - генерация производных колонок
- `test_pad_right_normalization` - нормализация кодов (удаление ведущих нулей, дополнение справа)
- `test_all_zeros_code` - обработка кодов из нулей

**Особенности:**
- Коды нормализуются: удаляются ведущие нули, затем дополняются справа до 10 знаков
- Например: `0000870421` → `8704210000`

### TestLoadTnvedMapping

Тестирует функцию `load_tnved_mapping`, которая загружает маппинги кодов ТН ВЭД из CSV и JSON файлов.

**Тесты:**
- `test_load_official_mappings` - загрузка официальных маппингов из CSV
- `test_load_translations` - загрузка переводов из JSON
- `test_uppercase_names` - преобразование названий в uppercase

**Структура файлов для тестов:**
```
metadata/
├── tnved.csv
└── translations/
    └── missing_codes_translations.json
```

**Формат tnved.csv:**
```csv
KOD,NAME,level
01,ЖИВЫЕ ЖИВОТНЫЕ,2
0101,ЛОШАДИ, ОСЛЫ, МУЛЫ И ЛОШАКИ,4
```

**Формат missing_codes_translations.json:**
```json
{
    "0101010000": {
        "russian_name": "Тестовое название"
    }
}
```

### TestLoadStranaMapping

Тестирует функцию `load_strana_mapping`, которая загружает маппинги кодов стран.

**Тесты:**
- `test_load_strana_mapping` - загрузка маппингов стран
- `test_case_insensitive_keys` - регистронезависимые ключи (uppercase)

**Формат STRANA.csv:**
```csv
KOD	NAME
RU	РОССИЯ
CN	КИТАЙ
```

### TestLoadCommonEdizmMapping

Тестирует функцию `load_common_edizm_mapping`, которая загружает маппинги единиц измерения.

**Тесты:**
- `test_load_edizm_mapping` - загрузка маппингов единиц измерения
- `test_aliases` - проверка алиасов (KG, КГ и т.д.)
- `test_uppercase_names` - преобразование названий в uppercase

**Формат edizm.csv:**
```csv
KOD,NAME
166,КИЛОГРАММ
796,ШТУКА
```

### TestSaveToDuckDB

Тестирует функцию `save_to_duckdb`, которая сохраняет DataFrame в DuckDB.

**Тесты:**
- `test_save_to_duckdb` - базовое сохранение в DuckDB
- `test_save_empty_dataframe` - обработка пустого DataFrame
- `test_save_with_chunking` - сохранение больших данных с чанкингом

**Особенности:**
- Тесты используют временные файлы через pytest fixture `tmp_path`
- PERIOD автоматически преобразуется в DATE тип
- Поддерживается чанкинг для больших данных

### TestIntegration

Интеграционные тесты, проверяющие взаимодействие нескольких функций.

**Тесты:**
- `test_schema_validation_with_generated_columns` - генерация колонок и валидация схемы
- `test_full_pipeline` - полный пайплайн: генерация → валидация → сохранение

## Использование pytest fixtures

Тесты используют встроенные pytest fixtures:

### tmp_path

Временная директория для каждого теста:

```python
def test_example(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("test content")
    # Файл автоматически удаляется после теста
```

## Добавление новых тестов

### Структура тестового класса

```python
class TestFunctionName:
    """Tests for function_name function."""
    
    def test_specific_case(self):
        """Test description."""
        # Arrange
        test_data = ...
        
        # Act
        result = function_name(test_data)
        
        # Assert
        assert result == expected_value
```

### Тестирование с временными файлами

```python
def test_load_file(tmp_path):
    """Test loading file."""
    # Создаем структуру директорий
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    
    # Создаем тестовый файл
    test_file = metadata_dir / "test.csv"
    test_file.write_text("KOD,NAME\n01,Test", encoding='utf-8')
    
    # Тестируем функцию
    project_root = tmp_path
    result = load_function(project_root)
    
    # Проверяем результат
    assert result is not None
```

### Тестирование с моками

```python
from unittest.mock import patch, MagicMock

def test_with_mock():
    """Test with mocked dependency."""
    with patch('module.external_function') as mock_func:
        mock_func.return_value = "mocked result"
        
        result = function_under_test()
        
        assert result == "expected"
        mock_func.assert_called_once()
```

## Лучшие практики

1. **Именование тестов**: используйте описательные имена, начинающиеся с `test_`
2. **Один тест - одна проверка**: каждый тест должен проверять одну конкретную вещь
3. **Arrange-Act-Assert**: структурируйте тесты по паттерну AAA
4. **Изоляция**: тесты должны быть независимыми и не зависеть от порядка выполнения
5. **Временные файлы**: используйте `tmp_path` для создания временных файлов
6. **Документация**: добавляйте docstrings к тестовым классам и методам

## Отладка тестов

### Запуск с отладочным выводом

```bash
pytest tests/ -v -s --pdb
```

### Запуск последнего упавшего теста

```bash
pytest tests/ --lf
```

### Запуск с остановкой на первой ошибке

```bash
pytest tests/ -x
```

### Просмотр подробной информации об ошибке

```bash
pytest tests/ -vv
```

## Покрытие кода

### Установка pytest-cov

```bash
pip install pytest-cov
```

### Генерация отчета о покрытии

```bash
# HTML отчет
pytest tests/ --cov=src --cov-report=html

# Консольный отчет
pytest tests/ --cov=src --cov-report=term-missing
```

Отчет будет доступен в `htmlcov/index.html`.

## CI/CD интеграция

Пример конфигурации для GitHub Actions:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install pytest pandas duckdb
      - name: Run tests
        run: |
          pytest tests/ -v
```

## Известные проблемы и ограничения

1. **Зависимости от файловой системы**: некоторые тесты требуют создания временных файлов
2. **DuckDB версии**: убедитесь, что версия DuckDB совместима с используемым API
3. **Кодировка**: все тестовые файлы должны использовать UTF-8 кодировку

## Полезные ресурсы

- [Документация pytest](https://docs.pytest.org/)
- [Pytest fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest best practices](https://docs.pytest.org/en/stable/goodpractices.html)

## Контакты

При возникновении проблем с тестами или вопросах по тестированию, обращайтесь к разработчикам проекта.

