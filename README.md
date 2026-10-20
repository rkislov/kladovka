# Кладовка

**Кладовка** — S3-совместимое объектное хранилище на чистом **Go** в **одном бинарнике**: локальное хранение, кластеризация с репликацией и поддержка **TLS/SSL**.

| Термин S3 | В Кладовке |
|-----------|------------|
| Bucket    | **Полка**  |
| Object    | Объект     |
| Key       | Ключ       |

Разработчик: **Роман Сергеевич Кислов**  
Лицензия: [Apache License 2.0](./LICENSE)

Репозиторий: [github.com/rkislov/kladovka](https://github.com/rkislov/kladovka)

---

## Документация

- [Инструкция администратора](docs/ADMIN.md): установка, безопасность, HTTPS, [подробная кластеризация и RF](docs/ADMIN.md#6-кластеризация-и-rf), проверка копий, отказы, миграция, мониторинг и резервные копии.
- [Инструкция пользователя](docs/USER.md): панель, файлы, AWS CLI, multipart, [работа пользователя с кластером](docs/USER.md#7-работа-с-кластером-и-rf), проверка результата и ошибки.
- [Состояние проекта](docs/STATUS.md): реализованные функции, проверки и оставшиеся ограничения.

Панель использует форму входа, ограниченные по времени сеансы и кнопку выхода. Метрики и JSON API поддерживают также HTTP Basic; `ADMIN_USER` / `ADMIN_PASSWORD` отделяют администратора от S3-клиентов. `ACCESS_POLICY_FILE` включает пользователей с несколькими ключами и ролями доступа к полкам; JSON API принимает также SigV4 с теми же ограничениями. Внутренний API использует Bearer-токен. Без файла доступа сохраняется совместимость с общей парой `ACCESS_KEY` / `SECRET_KEY`. Настройка и отзыв ключей описаны в [инструкции администратора](docs/ADMIN.md#пользователи-и-ключи). Перед сетевой эксплуатацией смените стандартные ключи и настройте HTTPS. Кластер пока требует фиксированного состава и не имеет автоматического восстановления потерянных копий.

## Рабочая версия 0.4.0-dev

Светлая адаптивная панель на локально собранном Tailwind CSS. Журнал незавершённых записей/удалений, восстановление при запуске, SHA-256-проверка чтения, квота данных, бюджет временных тел, ограничение параллельных загрузок и TTL multipart. Новый стабильный релиз этой рабочей версии пока не опубликован; предыдущий релиз — v0.3.0.

Изменение UI: `make ui` (Node.js 22+), затем `make build`. Обычная Go-сборка использует включённый CSS и не зависит от Node.js/CDN. Проверка AWS CLI и резервного восстановления: `python3 scripts/smoke.py --binary ./bin/kladovka` (отдельные временные каталоги).

## Возможности

- Path-style S3 API: ListBuckets / CreateBucket / PutObject / GetObject / HeadObject / DeleteObject / ListObjectsV2
- Multipart Upload (Create / UploadPart / Complete / Abort / ListParts)
- Полка = каталог на диске под `DATA_ROOT`
- AWS Signature Version 4 (заголовок `Authorization` и базовый presigned URL)
- Кластер: хеширование ключа по составу узлов по `полка/ключ`, primary + реплики, фоновый health пиров
- Межнодовый internal API (`/internal/health`, `/internal/object/...`)
- Встроенный веб-UI: `/ui/`
- JSON API полок и статистики: `/api/polki`, `/api/stats`; операции с файлами: `/api/object/<полка>/<ключ>`
- Панель: поиск полок, загрузка/скачивание/удаление файлов, страницы по 100 объектов
- **Метрики Prometheus**: `GET /metrics` (HTTP, S3, диск, кластер, репликация)
- **Prometheus HTTP SD**: `GET /internal/prometheus-sd`
- HTTPS: сертификат и ключ через переменные окружения (`TLS_CERT_FILE`, `TLS_KEY_FILE`)
- Один исполняемый файл `kladovka`

## Готовые сборки

Релизы на GitHub: [Releases](https://github.com/rkislov/kladovka/releases)

| Файл | Платформа |
|------|-----------|
| `kladovka-*-linux-amd64` | Linux x86_64 / серверы |
| `kladovka-*-linux-arm64` / `*-raspberrypi-64bit` | Raspberry Pi OS 64-bit, ARM64 |
| `kladovka-*-linux-armv7` / `*-raspberrypi-32bit` | Raspberry Pi 32-bit (armv7) |
| `kladovka-*-linux-armv6` | Старые Pi / armv6 |

Локальная кросс-сборка:

```bash
make release VERSION=0.4.0-dev
```

---

## Быстрый старт

```bash
git clone https://github.com/rkislov/kladovka.git
cd kladovka
make build
LISTEN_ADDR=127.0.0.1:8000 ./bin/kladovka
```

По умолчанию:

| Параметр | Значение |
|----------|----------|
| Слушает  | `:8000` |
| Данные   | `./data` |
| Access Key / Secret | `kladovka` / `kladovka-secret` |

Проверка (для стандартных тестовых ключей):

```bash
export ACCESS_KEY=kladovka
export SECRET_KEY=kladovka-secret
curl -s http://127.0.0.1:8000/healthz
curl -s --user "$ACCESS_KEY:$SECRET_KEY" http://127.0.0.1:8000/cluster
```

Пример с AWS CLI (path-style):

```bash
export AWS_ACCESS_KEY_ID=kladovka
export AWS_SECRET_ACCESS_KEY=kladovka-secret
export AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required

aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://documents
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./README.md s3://documents/readme.md
aws --endpoint-url http://127.0.0.1:8000 s3 ls s3://documents
```

В терминах Кладовки вы создали **полку** `documents` и положили на неё объект.

---

## Конфигурация (переменные окружения)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `LISTEN_ADDR` | `:8000` | Адрес HTTP(S) |
| `DATA_ROOT` | `./data` | Корень данных (полки) |
| `REGION` | `us-east-1` | Регион для SigV4 |
| `ACCESS_KEY` | `kladovka` | Access Key ID |
| `SECRET_KEY` | `kladovka-secret` | Secret Access Key в режиме общей пары |
| `ADMIN_USER`, `ADMIN_PASSWORD` | не заданы | Отдельные Basic-реквизиты администратора; задаются вместе |
| `SESSION_TTL_SEC` | `28800` | Абсолютный срок сеанса панели, 60–86400 секунд |
| `AUDIT_LOG_FILE` | `DATA_ROOT/.audit.jsonl` | JSONL-аудит действий и конфигурации доступа, права 0600 |
| `ACCESS_POLICY_FILE` | не задан | JSON-файл пользователей и S3-ключей, права 0600; при включении общая S3-пара отключена |
| `NODE_ID` | `node-1` | Идентификатор ноды |
| `NODE_URL` | `http://localhost:8000` | URL этой ноды для пиров |
| `PEER_NODES` | — | URL других нод через запятую |
| `REPLICATION_FACTOR` | `1` | Число копий объекта |
| `CLUSTER_VERIFY_TLS` | `true` | Проверять TLS при опросе пиров |
| `CLUSTER_TOKEN` | выводится из SECRET_KEY | Общий секрет внутреннего API; рекомендуется задать явно |
| `HEALTH_INTERVAL_SEC` | `10` | Интервал health-check |
| `MAX_OBJECT_SIZE` | `0` | Лимит размера объекта (0 = без лимита) |
| `TLS_CERT_FILE` | — | Путь к сертификату PEM |
| `TLS_KEY_FILE` | — | Путь к приватному ключу PEM |
| `TLS_MIN_VERSION` | `1.2` | `1.2` или `1.3` |

### HTTPS

```bash
export TLS_CERT_FILE=/etc/kladovka/tls.crt
export TLS_KEY_FILE=/etc/kladovka/tls.key
export NODE_URL=https://kladovka.example.com:8443
export LISTEN_ADDR=:8443
./bin/kladovka
```

### Кластер из двух нод

Нода A:

```bash
export NODE_ID=192.168.1.10:8000
export NODE_URL=http://192.168.1.10:8000
export PEER_NODES=http://192.168.1.11:8000
export REPLICATION_FACTOR=2
./bin/kladovka
```

Нода B:

```bash
export NODE_ID=192.168.1.11:8000
export NODE_URL=http://192.168.1.11:8000
export PEER_NODES=http://192.168.1.10:8000
export REPLICATION_FACTOR=2
./bin/kladovka
```

---

## API (кратко)

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/` | Список полок (ListBuckets) |
| `PUT` | `/<полка>` | Создать полку |
| `DELETE` | `/<полка>` | Удалить пустую полку |
| `GET` | `/<полка>?list-type=2` | Список объектов |
| `PUT` | `/<полка>/<ключ>` | Загрузить объект |
| `GET` | `/<полка>/<ключ>` | Скачать объект |
| `GET` | `/cluster` | Состояние кластера (JSON) |
| `GET` | `/healthz` | Liveness |
| `GET` | `/ui/` | Встроенный веб-интерфейс |
| `GET/POST` | `/api/polki` | Список / создание полок (JSON) |
| `GET` | `/metrics` | Метрики Prometheus |
| `GET` | `/internal/prometheus-sd` | HTTP SD для Prometheus |
| `GET` | `/internal/health` | Health для пиров |

### Метрики (Prometheus)

Пример: `monitoring/prometheus.yml` в репозитории.

Основные серии:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `kladovka_http_requests_total` | counter | HTTP-запросы |
| `kladovka_http_request_duration_seconds_*` | counter | Латентность |
| `kladovka_s3_operations_total` | counter | S3-операции (`operation`, `polka`) |
| `kladovka_s3_bytes_uploaded_total` | counter | Байты загрузки |
| `kladovka_s3_bytes_downloaded_total` | counter | Байты выдачи |
| `kladovka_storage_used_bytes` | gauge | Занято на ноде |
| `kladovka_cluster_node_healthy` | gauge | Здоровье нод |
| `kladovka_cluster_node_used_bytes` | gauge | Диск нод |
| `kladovka_replication_*_total` | counter | Репликация |

S3-операции требуют AWS Signature Version 4.

---

## Структура кода

```
cmd/kladovka/          — точка входа (один бинарник)
internal/config/       — конфигурация из env
internal/storage/      — полки и объекты на ФС
internal/cluster/      — hashing, registry, репликация
internal/auth/         — AWS SigV4
internal/s3api/        — HTTP API
internal/httpserver/   — HTTP / TLS
```

---

## Сборка и тесты

```bash
make build
make test
./bin/kladovka -version
```

Требуется Go 1.25+.

---

## UI и обои

Веб-UI (`/ui/`) — светлая панель на Tailwind CSS. Ранее подготовленные street-art обои сохранены отдельным wallpaper pack.

- Скриншоты: [`docs/screenshots/`](./docs/screenshots/)
- Wallpaper pack (1080p → 5K Retina, MacBook Retina): [`docs/wallpapers/`](./docs/wallpapers/)

![Светлая панель](./docs/screenshots/ui-light.png)

---

## Лицензия

Copyright © 2026 Роман Сергеевич Кислов

Licensed under the Apache License, Version 2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
