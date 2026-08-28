# Кабинет API-ключа внутри Superset

Страница «Мой API-ключ» для **существующих** пользователей Superset (без саморегистрации):
залогиненный пользователь **с нужной ролью** выпускает/перевыпускает свой токен, который
пишется в БД `tradeapi` (ту же, что читает API). Пользователей и роли заводит админ в Superset.

> Требует изменения деплоя Superset + **рестарт** (короткий даунтайм). Проверять на вашем
> инстансе — код версионно-зависим от Flask-AppBuilder; ниже — как поставить и как
> подстраховаться прямым URL, если меню не покажется.

## Файлы
- `api_cabinet.py` — FAB-view + синхронный (psycopg2) стор в `tradeapi`. Хеш токена совпадает
  с `api/app/store.py` (sha256), так что токены сразу валидны для API.
- `templates/api_cabinet.html`, `templates/api_cabinet_denied.html`.

## Установка на VPS

1. **Сделать модуль импортируемым в Superset.** `superset_config.py` лежит в каталоге,
   смонтированном в контейнер как `/app/pythonpath` (у вас — `~/superset`). Скопируйте туда же
   пакет:
   ```bash
   cp -r ~/mgimo-foreign-trade/superset_cabinet ~/superset/
   ls ~/superset/superset_cabinet    # api_cabinet.py, templates/
   ```
   (проверьте, что именно `~/superset` монтируется в `/app/pythonpath` — `docker inspect
   superset-superset-1 | grep pythonpath`.)

2. **Прописать в `~/superset/superset_config.py`:**
   ```python
   import os
   os.environ.setdefault("TRADEAPI_DSN", "postgresql://superset:superset@postgres:5432/tradeapi")
   os.environ.setdefault("TRADEAPI_ROLE", "API")      # роль-допуск

   def FLASK_APP_MUTATOR(app):
       from superset_cabinet.api_cabinet import register
       register(app)
   ```
   ⚠️ Если `FLASK_APP_MUTATOR` уже определён — не дублируйте, а вызовите `register(app)`
   внутри существующего.

3. **Создать роль-допуск** в Superset: Settings → List Roles → **добавить роль `API`**
   (можно без особых прав — наш view проверяет роль сам). Назначить её нужным пользователям
   (у них уже есть базовая роль для входа, напр. Gamma).

4. **Перезапустить Superset:**
   ```bash
   cd ~/superset && docker compose restart superset
   ```

## Как пользоваться

- URL страницы: **`http://217.26.28.186:8088/apikey/`** (прямой путь — работает независимо от
  меню). Залогиненный пользователь с ролью `API` жмёт «Выпустить токен», копирует его (показан
  один раз).
- Пункт меню «Настройки → Мой API-ключ» появляется, если роли выдать право
  **menu access on «Мой API-ключ»** (Settings → List Roles → API → Add permission). Если не
  выдавать — просто раздайте пользователям прямой URL `/apikey/`.

## Проверка

1. Зайти в Superset пользователем с ролью `API`, открыть `/apikey/`, «Выпустить токен».
2. Дёрнуть API этим токеном:
   ```bash
   curl -H "Authorization: Bearer mgt_..." \
     "http://<api-host>:8090/v1/trade?strana=CN&group_by=period&limit=3"
   ```
3. Пользователь без роли `API` на `/apikey/` видит сообщение об отказе; аноним →
   редирект на `/login/`.

## Заметки
- Superset уже ходит в `postgres` (та же сеть) — доступ к `tradeapi` есть из коробки; psycopg2
  в образе Superset присутствует.
- «Перевыпустить» отзывает предыдущий активный токен пользователя (rotate).
- Тариф всегда `pilot` (роль→тариф — на будущее, см. `docs/api-plan.md`).
