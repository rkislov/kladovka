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

## Возможности

- Path-style S3 API: ListBuckets / CreateBucket / PutObject / GetObject / HeadObject / DeleteObject / ListObjectsV2
- Полка = каталог на диске под `DATA_ROOT`
- AWS Signature Version 4 (заголовок `Authorization` и базовый presigned URL)
- Кластер: consistent hashing по `полка/ключ`, primary + реплики, фоновый health пиров
- Межнодовый internal API (`/internal/health`, `/internal/object/...`)
- HTTPS: сертификат и ключ через переменные окружения (`TLS_CERT_FILE`, `TLS_KEY_FILE`)
- Один исполняемый файл `kladovka`

---

## Быстрый старт

```bash
git clone https://github.com/rkislov/kladovka.git
cd kladovka
make build
./bin/kladovka
```

По умолчанию:

| Параметр | Значение |
|----------|----------|
| Слушает  | `:8000` |
| Данные   | `./data` |
| Access Key / Secret | `kladovka` / `kladovka-secret` |

Проверка:

```bash
curl -s http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/cluster
```

Пример с AWS CLI (path-style):

```bash
export AWS_ACCESS_KEY_ID=kladovka
export AWS_SECRET_ACCESS_KEY=kladovka-secret
export AWS_DEFAULT_REGION=us-east-1

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
| `SECRET_KEY` | `kladovka-secret` | Secret Access Key |
| `NODE_ID` | `node-1` | Идентификатор ноды |
| `NODE_URL` | `http://localhost:8000` | URL этой ноды для пиров |
| `PEER_NODES` | — | URL других нод через запятую |
| `REPLICATION_FACTOR` | `1` | Число копий объекта |
| `CLUSTER_VERIFY_TLS` | `true` | Проверять TLS при опросе пиров |
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
| `GET` | `/internal/health` | Health для пиров |

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

Требуется Go 1.22+.

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
