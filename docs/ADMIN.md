# Инструкция администратора — Кладовка

## Установка

1. Соберите бинарник: `make build` или скачайте релиз с GitHub.
2. Разместите `kladovka` на сервере (например `/usr/local/bin/kladovka`).
3. Задайте переменные окружения (см. README).
4. Запустите под systemd или в контейнере.

Пример unit-файла systemd:

```ini
[Unit]
Description=Кладовка object storage
After=network.target

[Service]
Type=simple
User=kladovka
Environment=DATA_ROOT=/var/lib/kladovka
Environment=LISTEN_ADDR=:8000
Environment=ACCESS_KEY=change-me
Environment=SECRET_KEY=change-me-too
Environment=NODE_ID=node-1
Environment=NODE_URL=http://127.0.0.1:8000
ExecStart=/usr/local/bin/kladovka
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## Безопасность

- Смените `ACCESS_KEY` / `SECRET_KEY` перед продакшеном.
- Включите TLS (`TLS_CERT_FILE`, `TLS_KEY_FILE`).
- Ограничьте доступ к `/internal/*` на уровне сети (firewall / private VLAN).
- Не публикуйте `/cluster` без необходимости.

## Кластер

- Все ноды должны иметь согласованный `REPLICATION_FACTOR` и полный список пиров в `PEER_NODES`.
- `NODE_ID` рекомендуется в виде `host:port`, совпадающем с `NODE_URL`.
- Для HTTPS между нодами используйте валидные сертификаты или временно `CLUSTER_VERIFY_TLS=false` только в лаборатории.

## Резервное копирование

Копируйте каталог `DATA_ROOT` (каждая полка — подкаталог). Предпочтительно при остановленной записи или снимке ФС.

---

Copyright 2026 Роман Сергеевич Кислов. Apache License 2.0.
