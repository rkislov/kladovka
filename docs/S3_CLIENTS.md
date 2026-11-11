# Проверенные S3-клиенты

Проверка: 20 октября 2026 года. Контрактные сценарии добавлены после v0.4.0. Исправление ключей с буквальным `%` также требует сборки из этой ветки; опубликованные бинарники v0.4.0 ещё не содержат его.

## Версии и область проверки

- AWS CLI: локально 2.34.42. В CI используется установленная версия runner; точная версия печатается в журнале каждого smoke-прогона. Это не обещание поддержки всех версий CLI.
- boto3 1.43.115 / botocore 1.43.115.
- MinIO Python SDK 7.2.20.

Версии Python SDK и их зависимостей закреплены в `scripts/requirements-s3-clients.txt`. CI использует Python 3.13. Проверки запускают собственные временные серверы на loopback: legacy RF=1 с общей парой ключей, legacy с локальной политикой доступа и трёхузловой consensus с запросами через ведомый узел. Данные, порты и тестовые ключи создаются только для прогона. Это функциональная проверка, а не нагрузочный пилот, TLS-проверка или сертификация полной спецификации S3.

## Что проверяется

AWS CLI проверяет создание/удаление полки, PUT/GET, автоматический multipart, LIST V2, Range, подписанный GET, сохранение SHA-256 после backup/restore. В consensus дополнительно проверяется подписанный запрос через ведомый узел с пробелом в имени объекта.

Совместный сценарий boto3 и MinIO проверяет:

- Создание, наличие, список и удаление полок.
- PUT одним клиентом и побайтовое чтение другим; пустой объект; HEAD/размер и Range.
- Кириллицу, пробел, плюс и процент в имени. Буквальный `a%2Fb` сохраняется отдельно от `a/b`, `%25` не декодируется повторно.
- Подписанные GET обоих клиентов.
- Настоящий multipart обоих SDK: объект больше 5 MiB, две части, ETag с суффиксом `-2`, чтение другим SDK и проверка байтов.
- Все страницы LIST V2 с размером страницы 1 через boto3 и рекурсивный список MinIO; точный набор имён без повторов.
- Отмену multipart, отсутствие незавершённого объекта, поштучное удаление объектов и пустую полку.
- Явный `NotImplemented` для CopyObject и DeleteObjects с проверкой, что исходный объект не изменился и копия не появилась.

## Настройки клиентов

Для AWS CLI используйте path-style адресацию, SigV4 и явный регион `us-east-1`. Для проверенного профиля optional checksums отключаются настройками `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` и `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`. Примеры команд — в [инструкции пользователя](USER.md).

Для boto3:

```python
import boto3
from botocore.config import Config

client = boto3.client(
    's3', endpoint_url='https://storage.example.org',
    # Реквизиты берутся из штатной цепочки credentials boto3.
    config=Config(
        region_name='us-east-1', signature_version='s3v4',
        s3={'addressing_style': 'path'},
        request_checksum_calculation='when_required',
        response_checksum_validation='when_required',
    ),
)
```

Для MinIO Python задавайте endpoint без схемы и явный `region='us-east-1'`, чтобы SDK не запрашивал неподдерживаемый GetBucketLocation. Для рабочей установки используйте `secure=True` и проверяемый TLS-сертификат. Реквизиты передавайте из конфигурации приложения, не сохраняйте их в исходниках. Используйте `list_objects(..., recursive=True)`: группировка по delimiter ещё не входит в проверенный профиль.

Справочники: [конфигурация boto3](https://docs.aws.amazon.com/boto3/latest/guide/configuration.html), [примеры MinIO Python SDK](https://github.com/minio/minio-py/tree/master/examples).

## Ограничения

CopyObject, DeleteObjects и GetBucketLocation возвращают NotImplemented. Для очистки используйте поштучный DeleteObject; для копирования — GET и PUT через приложение. Delimiter/CommonPrefixes, расширенная пагинация multipart, пользовательские metadata, versioning, ACL, lifecycle, tagging, streaming signatures и расширенные checksum-сценарии этим набором не подтверждаются. Наличие настройки checksum в SDK не означает, что сервер реализует все соответствующие проверки. Полные ограничения ключей и multipart приведены в USER.md.

Смена лидера во время незавершённой multipart-загрузки не входит в обещания: временные части не переносятся автоматически. Этот сценарий нельзя выводить из успешной загрузки через follower при стабильном лидере.

## Повторение

```bash
make build
python3 -m venv .venv-s3
.venv-s3/bin/python -m pip install -r scripts/requirements-s3-clients.txt
.venv-s3/bin/python scripts/smoke.py --binary ./bin/kladovka --sdk-clients
.venv-s3/bin/python scripts/smoke.py --binary ./bin/kladovka --sdk-clients --access-policy
.venv-s3/bin/python scripts/consensus_smoke.py --binary ./bin/kladovka --sdk-clients
```

AWS CLI должен быть доступен в PATH. Все сценарии завершаются ошибкой при несовпадении байтов, имён, ETag, статуса или ожидаемого отказа API. SDK-модуль не предоставляет режим подключения к рабочему endpoint; его вызывают только тестовые стенды. Журналы CI содержат версии и результат каждой группы проверок.
