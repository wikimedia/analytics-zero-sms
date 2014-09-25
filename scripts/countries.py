# coding=utf-8
import collections
import itertools
from pandas import json

from logprocessor import *

class CountriesScript(ScriptProcessor):
    def __init__(self, settingsFile='settings/countries.json'):
        super(CountriesScript, self).__init__(settingsFile, 'countries')

    def run(self):
        enabled = collections.defaultdict(int)
        disabled = collections.defaultdict(int)
        wiki = self.getWiki()
        for res in wiki.queryPages(generator='allpages', gaplimit='max', gapnamespace='480', prop='revisions', rvprop='content'):
            data = api.parseJson(res.revisions[0]['*'])
            if 'country' in data:
                # enabled by default
                if 'enabled' not in data or data.enabled:
                    enabled[data.country] += 1
                else:
                    disabled[data.country] += 1
        for state, vals in {'Enabled':enabled, 'Disabled':disabled}.iteritems():
            text = json.dumps([{"code":k, "val":v} for k,v in vals.iteritems()])
            wiki(
                'edit',
                title='Data:Json:StatsByCountry-' + state,
                summary='updating - %d countries' % (sum(vals.values())),
                text=text,
                token=wiki.token()
            )


if __name__ == '__main__':
    CountriesScript().safeRun()
