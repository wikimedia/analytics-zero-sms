import json
import requests as r
import api
from logprocessor import ScriptProcessor


class OperaIpUpdater(ScriptProcessor):
    def __init__(self, settingsFile='settings/opera.json'):
        super(OperaIpUpdater, self).__init__(settingsFile, 'opera')

    def run(self):
        res = r.get('https://ipranges.opera.com/mini/operaranges/opera_ip_ranges.json', proxies=self.proxyUrl)
        operaNets = sorted(set([v.subnet for v in api.parseJson(res)]))

        zerowiki = api.wikimedia('zero', 'wikimedia', 'https')
        if self.proxyUrl:
            zerowiki.session.proxies = self.proxyUrl

        zerowiki.login(self.settings.apiUsername, self.settings.apiPassword)

        title = 'Zero:-OPERA'
        res = next(zerowiki.queryPages(titles=title, prop='revisions', rvprop='content'))
        data = json.loads(res.revisions[0]['*'], object_hook=api.AttrDict)

        if sorted(set(data.ipsets.default)) != operaNets:
            data.ipsets.default = operaNets
            text = json.dumps(data, indent=True, sort_keys=True)

            zerowiki(
                'edit',
                title=title,
                summary='(bot) refreshing opera IPs',
                text=text,
                token=zerowiki.token()
            )

if __name__ == '__main__':
    OperaIpUpdater().safeRun()
