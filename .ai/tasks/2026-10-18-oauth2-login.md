# Přihlášení na Wikidata přes OAuth 2

Větev `123-oauth2-login`. Cíl: místo BotPassword (`wbi_login.Login`) se hlásit přes OAuth 2, aby se požadavky počítaly jako „authenticated“ podle https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits.

## Co je hotové
- `main.py` a `test_property_processor.py` používají `wbi_login.OAuth2` (WBI 0.12.15, grant client credentials proti `rest.php/oauth2/access_token`).
- `tools.get_oauth2_credentials()` čte soubor `oauth2_credentials`: 1. řádek client id, 2. řádek client secret. Soubor je v `.gitignore`.
- Relogin se spustí i na `mwoauth-invalid-authorization` (token z client credentials vyprší, WBI ho sám neobnovuje). Při obnově CSRF tokenu (po 1800 s) přijde expirace jako `LoginError`, ta relogin spouštěla už dřív.
- Token platí 4 h (`expires_in` 14400) a WBI ho posílá i se čtením (`wbgetentities`), proto `main.py` před každým řádkem CSV volá `token_expires_soon()` a 5 min před vypršením se přihlásí znovu.
- Ověřeno 2026-10-18: login prošel, uživatel Frettiebot (skupiny bot, user, autoconfirmed), scopes `basic, highvolume, editpage, createeditmovepage`, práva edit/bot/apihighlimits/createpage, CSRF token v pořádku, session nese cookies (`sessionJwt`). Načtení Q555628 přes OAuth2 session funguje.
- Zápisy po reloginu (`main.py`, větve `MWApiError` a `LoginError`) nově respektují `Config.debug`, dřív by zapsaly i v debug režimu.
- `get_bot_password()` odstraněna, nemá žádné volání. Soubor `bot_password` zůstává na disku, nepoužívá se.
- Zkušební běh 2026-10-18 na `output.csv` (2192 záznamů) s `debug=True`: doběhl za ~2,5 min bez chyb přihlášení, nic se nezapisovalo.
- První consumer měl jen scope `basic` a do souboru se omylem dal access token místo secretu – nový consumer to opravil.

## Co zbývá
- Případně zrušit první consumer (jen `basic`).
- Obnovení tokenu po 4 h zatím ověřené není, projeví se až při delším běhu.
- Pywikibot (`user-config.py`) zůstává na anonymním čtení, nic nezapisuje – neřešeno.

## Poznámka
Podle tabulky jsou účty s bot flagem z limitů úplně vyjmuté („Bot-flagged accounts … Exempt“) – to se ale projeví jen tehdy, když požadavky dorazí jako přihlášené. OAuth 2 session posílá i cookies, což stránka vyžaduje.

## 2026-10-19: client credentials nefungují pro zápis, přechod na trvalý access token
- Ostrý běh na plném dumpu (18.–19. 10.): SPARQL fáze ~14 h (s přestávkami kvůli spánku Macu), relogin po vypršení tokenu proběhl správně, ale **všechny zápisy (2) selhaly na `Invalid CSRF token.`**. Běh zastaven na řádku 734 000.
- Příčina: session z client credentials grantu (`wbi_login.OAuth2`) má CSRF token, který přestane platit během sekund (`action=checktoken`: hned ~50 % valid, po 5 s 0/8). Cookies ani GET/POST na tom nic nemění. Souvisí s T407987 / T420297 (client credentials v MediaWiki teprve dostávají přihlášení jako vlastník).
- Řešení: owner-only access token (JWT, platí do 3026) posílaný jako `Authorization: Bearer` přes `tools.oauth2_login()` → `wbi_login._Login(session=…)`. `checktoken` 8/8 valid, platný i po 2,5 min a po čteních. Token nevyprší, proto odstraněno `token_expires_soon()`.
- `oauth2_credentials` má nově 3 řádky (client id, secret, access token), čte se jen poslední; `get_oauth2_access_token()` odmítne soubor, který nekončí JWT.
- Při `badtoken` (commit d88f042) se bot znovu přihlásí a zápis zopakuje – dřív se `MWApiError` porovnávala přes `str(e)`, což je jen text, ne kód.
- Skutečný zápis s trvalým tokenem zatím neověřen: testovací běh na 2 úsecích čekal 35 min na maxlag (lag query service ~8,5 s > `maxlag=5`). Ověří se nočním během na Mac mini.
- 2026-10-19: `oauth2_credentials` (3 řádky) nahrán na Mac mini, `git pull` na `5d33219` hotový. Výchozí stav pro kontrolu: poslední editace s tagem `Czech-Authorities-Sync` je 2026-10-19T02:32:21Z (Q113846490, ještě BotPassword). Po běhu 20. 10. (launchd 14:08) hledat novější editace s tagem a v logu `Invalid CSRF token` / `MWApiError` / `Traceback`.
- 2026-10-19: běh na Mac mini ve 14:08 už s `5d33219`, ale spadl po Catmandu – `local_run.sh` hledal `.venv`, Mac mini má `venv` (Python 3.14). Skripty teď berou `.venv`, a když chybí, `venv`. Po pullu spuštěn ručně `main.py` nad hotovým `output.csv`.
- 2026-10-20: ruční běh na Mac mini (start 19. 10. 18:14, konec 3:24, `exit=0`) zapsal přes OAuth 2 9 editací s tagem, žádný `badtoken`. Jediná chyba zápisu: `permissiondenied` na Q47110477 – správce vrátil P569 1962 od Frettiebota a zamkl položku pro sysopy do 1. 9. 2027 („bots adding outdated info“). Q47110477 přidána do `qid_blacklist`.
- 2026-10-21: první plný launchd běh s OAuth 2 (20. 10. 14:08 – 21. 10. 1:02, 10 h 54 min) skončil `exit=0`. 6 editací s tagem, žádný `badtoken` / `MWApiError` / `LoginError`, Q47110477 přeskočena blacklistem. 68× `RemoteDisconnected` při zápisové fázi, WBI je opakuje po 60 s – zpomaluje, ale nevadí.
