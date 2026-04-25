# Railway Deploy — LeadKZ Free v7

## 1. На компьютере

Запусти:

```bat
START_HERE_WINDOWS.bat
```

Выбери пункт:

```text
1 — Полная настройка: .env + сессия + строка для Railway
```

После этого появится файл:

```text
railway_variables.txt
```

## 2. На Railway

1. Создай новый проект.
2. Подключи GitHub-репозиторий с этим проектом.
3. Открой **Variables**.
4. Скопируй туда все строки из `railway_variables.txt`.
5. Подключи **Volume**.
6. Mount path поставь:

```text
/data
```

7. Deploy.

## Важно

Не загружай в GitHub:

```text
.env
leadkz_session.session
session_string.txt
railway_variables.txt
```

Они уже добавлены в `.gitignore`.
