#!/bin/bash
#
# Přehled stavu launchd jobu launched.nkcr_catmandu — běží? kdy naposled?
# kdy příště? Záměrně bez `set -e`: chybějící log nebo nenačtený job není
# důvod skončit, jen se to má vypsat.
#
set -uo pipefail

LABEL="launched.nkcr_catmandu"
LOG_DIR="$HOME/Library/Logs/nkcr_catmandu"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

bold=$'\033[1m'; dim=$'\033[2m'; red=$'\033[31m'; grn=$'\033[32m'; off=$'\033[0m'
[ -t 1 ] || { bold=""; dim=""; red=""; grn=""; off=""; }

# sekundy -> "3 h 12 min" / "12 min"
human() {
    local s=$1
    if [ "$s" -ge 3600 ]; then echo "$((s / 3600)) h $(((s % 3600) / 60)) min"
    else echo "$((s / 60)) min"; fi
}

# `printf %-12s` počítá bajty, ne znaky, takže popisky s diakritikou
# ("Příště:", "Přeskočeno:") by se rozjely. Odtud vlastní zarovnání.
row() {
    local label="$1"; shift
    local pad=$(( 12 - ${#label} ))
    [ "$pad" -lt 1 ] && pad=1
    printf '%s%*s%s\n' "$label" "$pad" "" "$*"
}

printf '%s\n' "${bold}NK ČR → Wikidata pipeline${off}"
printf '%s\n' "${dim}──────────────────────────────────────────────${off}"

info="$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null)"
if [ -z "$info" ]; then
    row "Job:" "${red}není načtený v launchd${off}"
    row "" "launchctl bootstrap $DOMAIN $PLIST"
    exit 1
fi

# --- běží? ------------------------------------------------------------------
pid="$(awk '/^\tpid = /{print $3}' <<<"$info")"
if [ -n "$pid" ]; then
    # elapsed time procesu, ps ho dává jako [[dd-]hh:]mm:ss
    et="$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ')"
    row "Stav:" "${grn}BĚŽÍ${off} ${dim}(pid $pid, už $et)${off}"
else
    row "Stav:" "neběží"
fi

# --- poslední dokončený běh -------------------------------------------------
last="$(ls -1t "$LOG_DIR"/run-*.log "$LOG_DIR"/run-*.log.gz 2>/dev/null | head -1)"
if [ -z "$last" ]; then
    row "Naposled:" "${dim}zatím žádný běh${off}"
else
    line="$(zgrep -h 'konec, exit=' "$last" 2>/dev/null | tail -1)"
    if [ -n "$line" ]; then
        exit_code="$(sed -n 's/.*exit=\([0-9]*\).*/\1/p' <<<"$line")"
        when="$(sed -n 's/^=== \([0-9-]* [0-9:]*\) konec.*/\1/p' <<<"$line")"
        took="$(sed -n 's/.*trvalo \(.*\) ===/\1/p' <<<"$line")"
        if [ "$exit_code" = "0" ]; then verdict="${grn}v pořádku${off}"; else verdict="${red}CHYBA (exit=$exit_code)${off}"; fi
        row "Naposled:" "$when — $verdict, trvalo $took"
    else
        row "Naposled:" "${dim}$(basename "$last") — zatím bez závěru${off}"
    fi
fi

# --- příští naplánovaný běh -------------------------------------------------
h="$(/usr/libexec/PlistBuddy -c "Print :StartCalendarInterval:0:Hour" "$PLIST" 2>/dev/null)"
m="$(/usr/libexec/PlistBuddy -c "Print :StartCalendarInterval:0:Minute" "$PLIST" 2>/dev/null)"
if [ -n "$h" ] && [ -n "$m" ]; then
    hhmm="$(printf '%02d:%02d' "$h" "$m")"
    now="$(date +%s)"
    next="$(date -j -f '%Y-%m-%d %H:%M' "$(date '+%Y-%m-%d') $hhmm" +%s 2>/dev/null)"
    [ -n "$next" ] && [ "$next" -le "$now" ] && next=$((next + 86400))
    if [ -n "$next" ]; then
        row "Příště:" "$(date -r "$next" '+%a %-d. %-m. %H:%M') ${dim}(za $(human $((next - now))))${off}"
    fi
fi

# --- přeskočené běhy --------------------------------------------------------
if [ -s "$LOG_DIR/skipped.log" ]; then
    row "Přeskočeno:" "$(tail -1 "$LOG_DIR/skipped.log")"
fi

# --- co dál -----------------------------------------------------------------
printf '%s\n' "${dim}──────────────────────────────────────────────${off}"
if [ -n "$pid" ]; then
    cur="$(ls -1t "$LOG_DIR"/run-*.log 2>/dev/null | head -1)"
    if [ -n "$cur" ]; then
        printf '%s\n' "${dim}Poslední řádky logu:${off}"
        tail -3 "$cur" | sed 's/^/  /'
        printf '%s\n' "${dim}Sledovat živě:  tail -f $cur${off}"
    fi
else
    printf '%s\n' "${dim}Logy: $LOG_DIR${off}"
    printf '%s\n' "${dim}Spustit ručně:  launchctl kickstart $DOMAIN/$LABEL${off}"
fi
