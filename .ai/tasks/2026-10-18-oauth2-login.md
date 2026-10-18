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
