# nkcr_catmandu



## Pipeline

Dump Opendata NK (MARC XML) → catmandu na serveru → CSV s kompletním exportem důležitých polí z Autorit → Frettiebot/python skripty → Wikidata

## Logika

Podrobnější logika pro první fázi (pro aktuální stav prosím viz kód)

Nejprve - stáhnout si pomocné soubory:

Query 1: SPARQL dotaz na všechny nezavržené autority ve WD https://w.wiki/MLg

Query 2: SPARQL dotaz na všechny zavržené autority ve WD  zatím asi není třeba

Dále tyto kroky:

Skript 1: Pokud je v poli 024 vloženo QID → podívat se do této položky, je tam dané autoritní ID? (bez ohledu na rank - klidně i zavržený) → pokud ne, tak vložit do položky příslušné ID autorit z pole 024

Skript 2 (probíhá bez ohledu na výsledek skriptu 1): Je ID daného záznamu v nějaké položce na Wikidatech? Nejprve se podívej do Query 1 → pokud najdeš match, tak pokračuj do Skriptu 3. 

Skript 3 (jen pokud byl nalezen match ve skriptu 2) → načti si z matchované položky hodnoty vlastností ISNI, ORCID (bez ohledu na rank - klidně i zavržený) → pokud tyto hodnoty jsou odlišné od hodnoty v záznamu NK, tak vložit do položky příslušné ISNI a ORCID z pole 024 – Pokud je nkcr aut deprecated, přeskočit a nepokračovat do 3 (dle CSV).
