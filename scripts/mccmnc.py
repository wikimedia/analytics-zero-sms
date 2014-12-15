import json
import requests as r
import api
from logprocessor import ScriptProcessor
from netaddr import *
import _mysql
import sys

'''
This is not efficient. Let's just get it working first.

Usage: python mccmnc.py dbhost dbname mysql_cnf_path YYMMDD[HH...]
'''

class MccMncChecks(ScriptProcessor):
    def __init__(self, settingsFile='settings/mccmnc.json'):
        super(MccMncChecks, self).__init__(settingsFile, 'mccmnc')

    def run(self):
        zerowiki = self.getWiki()
        if self.proxyUrl:
            zerowiki.session.proxies = self.proxyUrl

        zerowiki.login(self.settings.apiUsername, self.settings.apiPassword)
        data = zerowiki('zeroportal', type='carriers')
        toRemove = []
        for xcs, subnets in data.items():
            if xcs == '310-260' or xcs.startswith('TEST'):
                toRemove.append(xcs)
        for i in toRemove:
            del data[i]

        db=_mysql.connect(host=sys.argv[1],db=sys.argv[2],read_default_file=sys.argv[3])
        db.query("""select event_ip,  event_mccMncNetwork, event_mccMncSim, count(*) from MobileWikiAppOperatorCode_8983918 where timestamp like '""" + sys.argv[4] + """%' group by event_ip,  event_mccMncNetwork, event_mccMncSim""")
        results = db.store_result()
        print ','.join(['supposed','network','sim','ip'])
        while True:
            record = results.fetch_row()
            if not record: break
            record = record[0]
            found = False
            for xcs, subnets in data.items():
                for subnet in subnets:
                    if IPAddress(record[0]) in IPNetwork(subnet):
                        found = True
                        if record[1] == xcs and record[2] == xcs:
                            break
                        print ','.join([xcs,record[1],record[2],record[0]])
                        break
                if found:
                    break
            if not found and (record[1] in data.keys() or record[2] in data.keys()):
                print ','.join(['unmapped',record[1],record[2],record[0]])
        db.close()

if __name__ == '__main__':
    MccMncChecks().safeRun()
