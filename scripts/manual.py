# coding=utf-8

from logprocessor import *

map = {
    "414-06": "MM",
    "520-18": "TH",
    "631-02": "AO",
    "426-04": "BH",
    "470-01": "BD",
    "470-03": "BD",
    "470-07": "BD",
    "652-02": "BW",
    "456-02": "KH",
    "624-02": "CM",
    "623-03": "CF",
    "630-86": "CG",
    "620-01": "GH",
    "630-03": "GH",
    "404-01": "IN",
    "510-11": "ID",
    "612-03": "CI",
    "416-03": "JO",
    "401-01": "KZ",
    "639-02": "KE",
    "639-03": "KE",
    "639-07": "KE",
    "293-41": "XK",
    "437-01": "KG",
    "646-02": "MG",
    "502-13": "MY",
    "502-16": "MY",
    "428-98": "MN",
    "297-01": "ME",
    "604-00": "MA",
    "429-02": "NP",
    "614-04": "NE",
    "621-20": "NG",
    "621-30": "NG",
    "410-01": "PK",
    "410-06": "PK",
    "515-03": "PH",
    "515-05": "PH",
    "250-99": "RU",
    "635-10": "RW",
    "420-01": "SA",
    "655-12": "ZA",
    "413-02": "LK",
    "436-01": "TJ",
    "436-04": "TJ",
    "436-05": "TJ",
    "514-02": "TP",
    "605-01": "TN",
    "641-14": "UG",
    "255-03": "UA",
}


class ManualScript(ScriptProcessor):
    def __init__(self, settingsFile='settings/manual.json', logDatePattern=False):
        super(ManualScript, self).__init__(settingsFile, 'manual')

    def run(self):
        wiki = self.getWiki()
        titles = ['Zero:' + v for v in map.keys()]
        for res in wiki.queryPages(titles=titles, prop='revisions', rvprop='content'):
            data = api.parseJson(res.revisions[0]['*'])
            code = map[res.title[len('Zero:'):]]
            if 'country' not in data:
                data.country = code
                wiki(
                    'edit',
                    title=res.title,
                    summary='updating country code',
                    text=json.dumps(data),
                    token=wiki.token()
                )


if __name__ == '__main__':
    ManualScript().safeRun()
