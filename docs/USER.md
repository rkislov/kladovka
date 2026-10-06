# Инструкция пользователя — Кладовка

## Что такое полка

**Полка** — это контейнер для объектов (в терминах Amazon S3 — *bucket*).  
Вы создаёте полку, кладёте на неё файлы (объекты) по ключу (пути).

## Веб-интерфейс

Откройте в браузере `http://<хост>:8000/ui/` — встроенная панель: состояние кластера и список полок.

## Веб / API

Кладовка отдаёт S3-совместимый API. Обычно администратор выдаёт:

- URL endpoint (например `https://kladovka.example.com:8443`)
- Access Key ID и Secret Access Key

## AWS CLI

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# создать полку
aws --endpoint-url "$ENDPOINT" s3 mb s3://my-shelf

# загрузить файл
aws --endpoint-url "$ENDPOINT" s3 cp ./photo.jpg s3://my-shelf/photos/photo.jpg

# список
aws --endpoint-url "$ENDPOINT" s3 ls s3://my-shelf

# скачать
aws --endpoint-url "$ENDPOINT" s3 cp s3://my-shelf/photos/photo.jpg ./photo.jpg
```

## Ошибки

Ответы об ошибках — XML с полями `Code` и `Message` (как у S3).  
Типичные коды: `AccessDenied`, `NoSuchBucket` (полка не найдена), `NoSuchKey`, `SignatureDoesNotMatch`.

---

Copyright 2026 Роман Сергеевич Кислов. Apache License 2.0.
