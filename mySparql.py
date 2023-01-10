from pywikibot.data.sparql import SparqlQuery, DEFAULT_HEADERS
from urllib.parse import quote
from typing import Optional
from pywikibot.backports import Dict, List
from requests.exceptions import Timeout
from contextlib import suppress
import rapidjson
from pywikibot.comms import http

try:
    from requests import JSONDecodeError
except ImportError:  # requests < 2.27.0
    from json import JSONDecodeError

class MySparqlQuery(SparqlQuery):

    def query(self, query: str, headers: Optional[Dict[str, str]] = None):
        """
        Run SPARQL query and return parsed JSON result.

        :param query: Query text
        """
        if headers is None:
            headers = DEFAULT_HEADERS

        url = '{}?query={}'.format(self.endpoint, quote(query))
        while True:
            try:
                self.last_response = http.fetch(url, headers=headers)
            except Timeout:
                self.wait()
                continue

            with suppress(JSONDecodeError):
                return rapidjson.loads(self.last_response.content)
            break

        return None