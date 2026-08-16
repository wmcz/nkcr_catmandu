# Analýza výpisu nenalezených pojmů (not_found_occupations / not_found_places)

Datum: 2026-08-16
Stav: analýza, žádné změny v kódu

## Zadání

Na konci běhu se vypisuje seznam nenalezených pojmů s počty výskytů
(`main.py:337-338`). Otázka: dá se s tím něco dělat automaticky — např. zakládat
nové položky na Wikidatech — nebo je nutné dohledávat ručně?

## Co ten výpis je

- `context.not_found_occupations` a `context.not_found_places` (`context.py:51-66`),
  plněné z `cleaners.prepare_occupation_from_nkcr()` a `prepare_places_from_nkcr()`.
- Klíč = řetězec z NK ČR (pole 374a / 372a / 370a-b-f), hodnota = počet výskytů.
- Na konci jen `print()` — neseřazené, neukládá se do souboru.
- **Jazyky se netrackují vůbec** — `prepare_language_from_nkcr()` (`cleaners.py:145`)
  jen loguje warning, do žádného počítadla nezapisuje.

## Jak vzniká „nenalezeno“

Slovník `context.name_to_nkcr` staví `tools.get_occupations()`: bere z Wikidat
položky s `P691` (NK ČR AUT ID) začínajícím ph/fd/ge/xx a **kvalifikátorem P1810**,
a mapuje `P1810 string → QID`. Pojem je tedy „nenalezen“ tehdy, když na Wikidatech
neexistuje položka s P691 + P1810 přesně v tomto tvaru. To má několik různých příčin
s velmi různým řešením.

## Zjištěné kategorie (na základě catmandu.log a kontroly proti živé Wikidatě)

1. **Falešné poplachy z nestabilního stránkování.**
   `get_occupations()` (`tools.py:268-275`) stránkuje přes `LIMIT/OFFSET`
   **bez `ORDER BY`**. WDQS pak negarantuje stabilní pořadí a každý běh dostane
   jinou podmnožinu. Doklad z logu — velikost slovníku mezi běhy kolísá:
   199741 / 201732 / 201762 / 202423 / 201875 / 201999 / 201471.
   Stejná třída chyby, jaká se u `non_deprecated_items` řešila commitem 0d53ec1
   (přechod z OFFSET na VALUES batche).

2. **Chyba v parsování jazyků.** V logu jsou pojmy `cze|cze`, `eng|eng`, `eng|cze`.
   Catmandu fix spojuje podpole 377a znakem `$` a opakovaná pole znakem `|`
   (`biografickapole-pro-frettieho.fix`), ale `prepare_language_from_nkcr()`
   splituje jen na `$` (`cleaners.py:137`). Kódy `cze` i `eng` v `jazyky.csv` jsou,
   selhává jen ten split. Stejná past hrozí u 370f (splituje `|` jen když `$` chybí).

3. **Chybějící kódy v `jazyky.csv`** — `mul`, `scr`, `mol`, `jap`, `esp`, `gag`.
   (`syr` už doplněn je, Q33538.)

4. **Existující položka bez správné varianty zápisu.** Např.
   „Halle (Saale, Německo)“ vs. „Halle an der Saale (Německo)“, „Praha“ bez
   upřesnění (ve slovníku je jen „Praha (Česko)“), ženské tvary
   („vysokoškolské pedagožky“). Řešení = doplnit P1810 na **existující** položku,
   ne zakládat novou.

5. **Reálně chybějící pojem** — např. „filozofie ekonomiky“. Až tady dává smysl
   uvažovat o nové položce.

Kontrola tří historicky nejčastějších pojmů proti živé Wikidatě (16. 8. 2026):
`filozofie` → Q5891/ph114540, `psychologie` → Q9418/ph115948,
`urbanismus` → Q59950/ph116541 — všechny dnes napojené jsou. Historické počty
z logu jsou tedy z velké části již vyřešené.

**Pozor na čtení statistik z `catmandu.log`**: soubor míchá běhy z let 2024–2026
a poslední ostrý běh v něm je starý — novější běhy byly krátké testy nad `test.csv`.
Souhrnná čísla (36 720 nenalezených povolání, 4 604 míst) tedy neodpovídají
aktuálnímu stavu.

## Doporučený postup (od nejlevnějšího k nejdražšímu)

1. Opravit stránkování `get_occupations()` — jinak se ručně dohledávají fantomy.
2. Opravit split 377a (a prověřit 370f), doplnit chybějící kódy do `jazyky.csv`.
3. Výpis ukládat do CSV seřazeného podle počtu, ne `print()`; přidat tracking jazyků.
4. Triage skript (read-only vůči Wikidatům): pro každý nenalezený pojem najít
   kandidátskou položku a zařadit do kategorie „doplnit P1810“ / „doplnit P691“ /
   „položka opravdu chybí“. Výstup jako podklad pro QuickStatements.
5. Nové položky zakládat jen ručně/po kontrole z kategorie 5 — plošné automatické
   zakládání by dělalo duplicity, protože většina pojmů na Wikidatech už existuje.
