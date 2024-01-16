import json
import uuid

import pywikibot
from pywikibot.page._collections import (
    ClaimCollection,
)
from pywikibot.site._decorators import need_right


class MyItemPage(pywikibot.ItemPage):
    DATA_ATTRIBUTES = {
        'claims': ClaimCollection,
    }


class MyDataSite(pywikibot.site.DataSite):
    @need_right('edit')
    def addClaim(self, entity, claim, bot=True, summary=None, tags=None):
        """
        Add a claim.

        :param tags:
        :param entity: Entity to modify
        :type entity: WikibaseEntity
        :param claim: Claim to be added
        :type claim: pywikibot.Claim
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        """
        if tags is None:
            tags = []
        claim.snak = entity.getID() + '$' + str(uuid.uuid4())
        params = {'action': 'wbsetclaim',
                  'claim': json.dumps(claim.toJSON()),
                  'baserevid': entity.latest_revision_id,
                  'summary': summary,
                  'token': self.tokens['edit'],
                  'bot': bot,
                  'tags': tags
                  }
        req = self._simple_request(**params)
        jsonvys = claim.toJSON()
        data = req.submit()
        # Update the item
        if claim.getID() in entity.claims:
            entity.claims[claim.getID()].append(claim)
        else:
            entity.claims[claim.getID()] = [claim]
        entity.latest_revision_id = data['pageinfo']['lastrevid']

    @need_right('edit')
    def editSource(self, claim, source, new=False,
                   bot=True, summary=None, baserevid=None, tags=None):
        """
        Create/Edit a source.

        :param tags:
        :param claim: A Claim object to add the source to
        :type claim: pywikibot.Claim
        :param source: A Claim object to be used as a source
        :type source: pywikibot.Claim
        :param new: Whether to create a new one if the "source" already exists
        :type new: bool
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        :param baserevid: Base revision id override, used to detect conflicts.
            When omitted, revision of claim.on_item is used. DEPRECATED.
        :type baserevid: long
        """
        if tags is None:
            tags = []
        if claim.isReference or claim.isQualifier:
            raise ValueError('The claim cannot have a source.')
        params = {'action': 'wbsetreference', 'statement': claim.snak,
                  'baserevid': claim.on_item.latest_revision_id,
                  'summary': summary, 'bot': bot, 'token': self.tokens['edit'], 'tags': tags}

        # build up the snak
        if isinstance(source, list):
            sources = source
        else:
            sources = [source]

        snak = {}
        for sourceclaim in sources:
            datavalue = sourceclaim._formatDataValue()
            valuesnaks = []
            if sourceclaim.getID() in snak:
                valuesnaks = snak[sourceclaim.getID()]
            valuesnaks.append({'snaktype': 'value',
                               'property': sourceclaim.getID(),
                               'datavalue': datavalue,
                               },
                              )

            snak[sourceclaim.getID()] = valuesnaks
            # set the hash if the source should be changed.
            # if present, all claims of one source have the same hash
            if not new and hasattr(sourceclaim, 'hash'):
                params['reference'] = sourceclaim.hash
        params['snaks'] = json.dumps(snak)

        req = self._simple_request(**params)
        return req.submit()

    @need_right('edit')
    def editQualifier(self, claim, qualifier, new=False, bot=True,
                      summary=None, baserevid=None, tags=None):
        """
        Create/Edit a qualifier.

        :param new:
        :param tags:
        :param claim: A Claim object to add the qualifier to
        :type claim: pywikibot.Claim
        :param qualifier: A Claim object to be used as a qualifier
        :type qualifier: pywikibot.Claim
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        :param baserevid: Base revision id override, used to detect conflicts.
            When omitted, revision of claim.on_item is used. DEPRECATED.
        :type baserevid: long
        """
        if tags is None:
            tags = []
        if claim.isReference or claim.isQualifier:
            raise ValueError('The claim cannot have a qualifier.')
        params = {'action': 'wbsetqualifier', 'claim': claim.snak,
                  'baserevid': claim.on_item.latest_revision_id,
                  'summary': summary, 'bot': bot, 'tags': tags}

        if (not new and hasattr(qualifier, 'hash')
                and qualifier.hash is not None):
            params['snakhash'] = qualifier.hash
        params['token'] = self.tokens['edit']
        # build up the snak
        if qualifier.getSnakType() == 'value':
            params['value'] = json.dumps(qualifier._formatValue())
        params['snaktype'] = qualifier.getSnakType()
        params['property'] = qualifier.getID()

        req = self._simple_request(**params)
        return req.submit()
