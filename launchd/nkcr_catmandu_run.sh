#!/bin/bash
#
# Wrapper spouštěný launchd jobem launched.nkcr_catmandu (viz plist vedle).
#
# Obaluje local_run.sh o tři věci, které v něm samotném být nemůžou nebo
# nedávají smysl, protože local_run.sh je sdílený a launchd je jen macOS:
#
#   1. rotaci logů — launchd do StandardOutPath/StandardErrorPath jen
#      připisuje a nikdy nerotuje, takže log neomezeně roste
#   2. zámek proti souběžnému běhu (launchd sice druhou instanci téhož jobu
#      nespustí, ale ruční spuštění vedle naplánovaného ano)
#   3. errexit pro local_run.sh — přes `bash -e`, tedy bez jeho editace
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$HOME/Library/Logs/nkcr_catmandu"
LOCK="$LOG_DIR/run.lock"
KEEP=7   # kolik posledních běhů si nechat (komprimovaně)

mkdir -p "$LOG_DIR"

# --- zámek proti souběhu ----------------------------------------------------
# mkdir je atomický; PID uvnitř umožní poznat zámek zapomenutý po pádu/rebootu.
if ! mkdir "$LOCK" 2>/dev/null; then
    lock_pid="$(cat "$LOCK/pid" 2>/dev/null || true)"
    if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
        echo "$(date '+%F %T') už běží (pid $lock_pid), přeskakuji" >>"$LOG_DIR/skipped.log"
        exit 0
    fi
    echo "$(date '+%F %T') odstraňuji zapomenutý zámek (pid '$lock_pid')" >>"$LOG_DIR/skipped.log"
    rm -rf "$LOCK"
    mkdir "$LOCK"
fi
echo $$ >"$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT

# --- logy -------------------------------------------------------------------
# Každý běh má vlastní soubor, takže rotace = zabalit starší a smazat přebytek.
LOG="$LOG_DIR/run-$(date '+%Y%m%d-%H%M%S').log"
exec >>"$LOG" 2>&1

# Pozor: když ještě žádný log neexistuje, `ls` skončí nenulově a `pipefail`
# to propaguje na celý pipe — pod `set -e` by první běh umřel právě tady,
# ještě před spuštěním pipeline, a trap by uvolnil zámek. Odtud `|| true`.
list_logs() { ls -1t "$LOG_DIR"/$1 2>/dev/null || true; }

list_logs 'run-*.log' | tail -n +2 | while read -r old; do
    gzip -f "$old" || true
done
list_logs 'run-*.log.gz' | tail -n +$((KEEP + 1)) | while read -r old; do
    rm -f "$old"
done

# --- vlastní běh ------------------------------------------------------------
# local_run.sh začíná nepodmíněným `rm aut.xml.gz; rm aut.xml`. Pod `bash -e`
# by první chybějící soubor celý skript shodil, takže je předem vytvoříme.
touch "$REPO/aut.xml.gz" "$REPO/aut.xml"

echo "=== $(date '+%F %T') start ==="
started=$(date +%s)
status=0
/bin/bash -e "$REPO/local_run.sh" || status=$?
elapsed=$(( $(date +%s) - started ))
echo "=== $(date '+%F %T') konec, exit=$status, trvalo $((elapsed / 3600)) h $(((elapsed % 3600) / 60)) min ==="

exit $status
