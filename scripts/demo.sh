#!/usr/bin/env bash
# Показ всего цикла: запись, повтор, откат, очередь.
# Требует запущенного сервиса: make run в соседнем окне.
set -euo pipefail
API="${API:-http://localhost:8000}"

say() { printf "\n\033[1m%s\033[0m\n" "$1"; }

say "1. Записываем остатки"
RESPONSE=$(curl -sS -X POST "$API/v1/stock" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-001' \
  -d '{"updates":[{"sku":"ART-0042","warehouse":"spb-1","quantity":120,"reason":"поставка"}],"actor":"demo"}')
echo "$RESPONSE"
BATCH=$(printf '%s' "$RESPONSE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["batch_id"])')

say "2. Тот же вебхук повторно: остаток не должен уехать второй раз"
curl -sS -X POST "$API/v1/stock" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-001' \
  -d '{"updates":[{"sku":"ART-0042","warehouse":"spb-1","quantity":120,"reason":"поставка"}],"actor":"demo"}' \
  -D - -o /dev/null | grep -i 'idempotent-replay' || true

say "3. Журнал изменений"
curl -sS "$API/v1/changes?sku=ART-0042"

say "4. Откат партии $BATCH"
curl -sS -X POST "$API/v1/batches/$BATCH/revert"

say "5. Очередь событий"
curl -sS "$API/v1/outbox"

say "6. Здоровье"
curl -sS "$API/health"
echo
