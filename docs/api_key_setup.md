# Настройка API ключа OpenAI

## Способ 1: Файл .env (Рекомендуется)

Самый безопасный и удобный способ - использовать файл `.env` в корне проекта.

### Шаги:

1. **Создайте файл `.env` в корне проекта:**
   ```
   F:\YandexDisk\HSE\mgimo-foreign_trade\.env
   ```

2. **Добавьте в файл:**
   ```
   OPENAI_API_KEY=your-actual-api-key-here
   ```

3. **Установите python-dotenv (если еще не установлен):**
   ```bash
   pip install python-dotenv
   ```

4. **Готово!** Скрипт автоматически загрузит ключ из `.env` файла.

### Важно:
- Файл `.env` уже добавлен в `.gitignore`, поэтому ключ не попадет в git
- Никогда не коммитьте файл `.env` в репозиторий
- Не передавайте файл `.env` другим людям

---

## Способ 2: Переменные окружения

### Windows PowerShell:
```powershell
$env:OPENAI_API_KEY="your-actual-api-key-here"
python src/translate_missing_codes.py --test 5
```

### Windows CMD:
```cmd
set OPENAI_API_KEY=your-actual-api-key-here
python src/translate_missing_codes.py --test 5
```

### Linux/Mac:
```bash
export OPENAI_API_KEY="your-actual-api-key-here"
python src/translate_missing_codes.py --test 5
```

**Недостаток:** Ключ действует только в текущей сессии терминала.

---

## Способ 3: Постоянные переменные окружения (Windows)

### Через GUI:
1. Откройте "Параметры системы" → "Дополнительные параметры системы"
2. Нажмите "Переменные среды"
3. В "Переменные пользователя" нажмите "Создать"
4. Имя: `OPENAI_API_KEY`
5. Значение: ваш API ключ
6. Нажмите OK

### Через PowerShell (от администратора):
```powershell
[System.Environment]::SetEnvironmentVariable('OPENAI_API_KEY', 'your-key-here', 'User')
```

После этого перезапустите терминал.

---

## Проверка установки

После настройки ключа проверьте:

```bash
python src/translate_missing_codes.py --test 1
```

Если ключ установлен правильно, скрипт начнет перевод. Если нет - увидите сообщение с инструкциями.

---

## Безопасность

⚠️ **ВАЖНО:**
- Никогда не храните API ключи в коде
- Не коммитьте файлы с ключами в git
- Не передавайте ключи через незащищенные каналы
- Используйте `.env` файл и убедитесь, что он в `.gitignore`

