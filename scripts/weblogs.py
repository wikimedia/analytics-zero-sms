# coding=utf-8
import StringIO
import gzip
import re
import collections
import sys
from pandas import read_table, pivot_table
from pandas.core.frame import DataFrame
import numpy as np
import api

from logprocessor import *


def addStat(stats, date, dataType, xcs, via, ipset, https, lang, subdomain, site):
    key = (date, dataType, xcs, via, ipset, 'https' if https else 'http', lang, subdomain, site)
    if key in stats:
        stats[key] += 1
    else:
        datetime.strptime(date, '%Y-%m-%d')  # Validate date - slow operation, do it only once per key
        stats[key] = 1


columnHeaders10 = u'date,type,xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHeaders11 = u'date,type,xcs,via,ipset,https,lang,subdomain,site,iszero,count'.split(',')
validSubDomains = {'m', 'zero', 'mobile', 'wap'}
validHttpCode = {'200', '304'}


class WebLogProcessor(LogProcessor):
    def __init__(self, settingsFile='settings/weblogs.json', logDatePattern=False):
        super(WebLogProcessor, self).__init__(settingsFile, 'web')

        self.enableUpload = not logDatePattern
        # zero.tsv.log-20140808.gz
        if not logDatePattern:
            logDatePattern = r'\d+'
        logReStr = r'zero\.tsv\.log-(' + logDatePattern + ')\.gz'
        self.logFileRe = re.compile(r'^' + logReStr + r'$', re.IGNORECASE)
        self.statFileRe = re.compile(r'^(' + logReStr + r')__\d+\.tsv$', re.IGNORECASE)
        self.urlRe = re.compile(r'^https?://([^/]+)', re.IGNORECASE)
        self.duplUrlRe = re.compile(r'^(https?://.+)\1', re.IGNORECASE)
        self.zcmdRe = re.compile(r'zcmd=([-a-z0-9]+)', re.IGNORECASE)
        self.combinedFile = os.path.join(self.pathCache, 'combined-all.tsv')
        self._wiki = None

    def downloadConfigs(self):
        wiki = self.getWiki()
        # https://zero.wikimedia.org/w/api.php?action=zeroportal&type=analyticsconfig&format=jsonfm
        configs = wiki('zeroportal', type='analyticsconfig').zeroportal
        for cfs in configs.values():
            for c in cfs:
                c['from'] = datetime.strptime(c['from'], '%Y-%m-%dT%H:%M:%SZ')
                if c.before is None:
                    c.before = datetime.max
                else:
                    c.before = datetime.strptime(c.before, '%Y-%m-%dT%H:%M:%SZ')
                c.languages = True if True == c.languages else set(c.languages)
                c.sites = True if True == c.sites else set(c.sites)
                c.via = set(c.via)
                c.ipsets = set(c.ipsets)
        return configs

    def processLogFiles(self):

        safePrint('Processing log files')
        statFiles = {}
        for f in os.listdir(self.pathLogs):
            m = self.logFileRe.match(f)
            if not m:
                continue
            logFile = os.path.join(self.pathLogs, f)
            logSize = os.stat(logFile).st_size
            statFile = os.path.join(self.pathCache, f + '__' + unicode(logSize) + '.tsv')
            statFiles[f] = statFile
            if not os.path.exists(statFile):
                fileDt = m.group(1)
                fileDt = '-'.join([fileDt[0:4], fileDt[4:6], fileDt[6:8]])
                self.processLogFile(logFile, statFile, fileDt)
                if os.path.isfile(self.combinedFile):
                    os.remove(self.combinedFile)

        # Clean up older stat files (if gz file size has changed)
        removeFiles = []
        for f in os.listdir(self.pathCache):
            m = self.statFileRe.match(f)
            if not m:
                continue
            logFile = m.group(1)
            statFile = os.path.join(self.pathCache, f)
            if logFile not in statFiles or statFiles[logFile] == statFile:
                continue  # The log file has been deleted or its the latest
            removeFiles.append(statFile)
        for f in removeFiles:
            os.remove(f)

    def processLogFile(self, logFile, statFile, fileDt):
        """
            0  cp1046.eqiad.wmnet
            1  13866141087
            2  2014-08-07T06:30:46
            3  0.000130653
            4  <ip>
            5  hit/301
            6  0
            7  GET
            8  http://en.m.wikipedia.org/wiki/Royal_Challenge
            9  -
            10 text/html; charset=UTF-8
            11 http://en.m.wikipedia.org/wiki/Royal_Challenge
            12 -
            13 Mozilla/5.0 (Linux; U; Android 2.3.5; en-us; ...
            .. Version/4.0 Mobile Safari/534.30
            -2 en-US
            -1 zero=410-01
        """

        safePrint('Processing %s' % logFile)
        stats = {}
        count = 0

        if logFile.endswith('.gz'):
            streamData = io.TextIOWrapper(io.BufferedReader(gzip.open(logFile)), encoding='utf8', errors='ignore')
        else:
            streamData = io.open(logFile, 'r', encoding='utf8', errors='ignore')
        for line in streamData:
            count += 1
            if count % 1000000 == 0:
                safePrint('%d lines processed' % count)

            l = line.strip('\n\r').split('\t')

            if len(l) < 16:
                safePrint(u'String too short - %d parts\n%s' % (len(l), line))
                addStat(stats, fileDt, 'ERR', '000-00', 'ERR', 'ERR', False, '', 'short-str', str(len(l)))
                continue
            analytics = l[-1]
            if '=' not in analytics:  # X-Analytics should have at least some values
                safePrint(u'Analytics is not valid - "%s"\n%s' % (analytics, line))
                addStat(stats, fileDt, 'ERR', '000-00', 'ERR', 'ERR', False, '', 'analytics', '')
                continue
            verb = l[7]
            analytics = AttrDict([x.split('=', 2) for x in set(analytics.split(';'))])
            if 'zero' in analytics:
                xcs = analytics['zero'].rstrip('|')
            else:
                xcs = None
            tmp = l[5].split('/')
            if len(tmp) != 2:
                safePrint(u'Invalid status - "%s"\n%s' % (l[5], line))
                addStat(stats, fileDt, 'ERR', '000-00', 'ERR', 'ERR', False, '', 'status', '')
                continue
            (cache, httpCode) = tmp
            via = analytics['proxy'].upper() if 'proxy' in analytics else 'DIRECT'
            ipset = analytics['zeronet'] if 'zeronet' in analytics else 'default'
            https = 'https' in analytics
            dt = l[2]
            dt = dt[0:dt.index('T')]

            url = l[8]
            if url.find('http', 1) > -1:
                m = self.duplUrlRe.match(url)
                if m:
                    url = url[len(m.group(1)):]
            m = self.urlRe.match(url)
            if not m:
                safePrint(u'URL parsing failed: "%s"\n%s' % (url, line))
                addStat(stats, fileDt, 'ERR', xcs, via, ipset, https, '', 'url', '')
                continue
            host = m.group(1)
            if host.endswith(':80'):
                host = host[:-3]
            if host.endswith('.'):
                host = host[:-1]
            hostParts = host.split('.')
            if hostParts[0] == 'www':
                del hostParts[0]
            lang = ''
            subdomain = ''
            if len(hostParts) >= 2:
                hostParts.pop()  # assume last element is the domain root, e.g. org, net, info, net, ...
                site = hostParts.pop()
                if hostParts:
                    subdomain = hostParts.pop()
                    if subdomain in validSubDomains:
                        lang = hostParts.pop() if hostParts else ''
                    else:
                        lang = subdomain
                        subdomain = ''
            else:
                hostParts = False
                site = ''

            if hostParts or False == hostParts:
                safePrint(u'Unknown host %s\n%s' % (host, line))
                addStat(stats, fileDt, 'ERR', xcs, via, ipset, https, '', 'host', host)
                continue

            addStat(stats, dt, 'STAT', xcs, via, ipset, https, '', 'cache', cache)
            addStat(stats, dt, 'STAT', xcs, via, ipset, https, '', 'verb', verb)
            addStat(stats, dt, 'STAT', xcs, via, ipset, https, '', 'ret', httpCode)

            if 'ZeroRatedMobileAccess' in url and 'zcmd' in url:
                m = self.zcmdRe.search(url)
                addStat(stats, dt, 'STAT', xcs, via, ipset, https, '', 'zcmd', m.group(1) if m else '?')
                continue
            if httpCode not in validHttpCode:
                continue
            if verb != 'GET':
                continue

            # Valid request!
            addStat(stats, dt, 'DATA', xcs, via, ipset, https, lang, subdomain, site)

        writeData(statFile, [list(k) + [v] for k, v in stats.iteritems()], columnHeaders10)

    def combineStats(self):
        safePrint('Combine stat files')
        # Logs did not contain the "VIA" X-Analytics tag before this date
        ignoreViaBefore = datetime(2014, 3, 22)
        configs = self.downloadConfigs()
        stats = collections.defaultdict(int)
        for f in os.listdir(self.pathCache):
            if not self.statFileRe.match(f):
                continue
            for vals in readData(os.path.join(self.pathCache, f), columnHeaders10):
                # "0          1    2      3      4       5    6  7    8         9"
                # "2014-07-25 DATA 250-99 DIRECT default http ru zero wikipedia 1000"
                if len(vals) != 10:
                    if len(vals) == 11 and vals[3] == '':
                        safePrint('Fixing extra empty xcs in file %s' % f)
                        del vals[3]
                    else:
                        raise ValueError('Unrecognized key (%s) in file %s' % (joinValues(vals), f))
                (dt, typ, xcs, via, ipset, https, lang, subdomain, site, count) = vals

                error = False

                if typ == 'DATA' and site not in validSites:
                    error = 'bad-site'
                elif xcs in configs:
                    if xcs == '404-01b':
                        vals[2] = xcs = '404-01'
                        vals[4] = ipset = 'b'
                    if typ == 'DATA':
                        dt = datetime.strptime(dt, '%Y-%m-%d')
                        site2 = subdomain + '.' + site
                        isZero = False
                        for conf in configs[xcs]:
                            langs = conf.languages
                            sites = conf.sites
                            if conf['from'] <= dt < conf.before and \
                                    (conf.https or https == u'http') and \
                                    (True == langs or lang in langs) and \
                                    (True == sites or site2 in sites) and \
                                    (dt < ignoreViaBefore or via in conf.via) and \
                                    (ipset in conf.ipsets):
                                isZero = True
                                break
                        vals[9] = u'yes' if isZero else u'no'
                    else:
                        vals[9] = ''
                else:
                    # X-CS does not exist, ignore it
                    error = 'xcs'

                if error:
                    vals = (vals[0], 'ERR', '000-00', 'ERR', 'ERR', 'http', '', error, '', '')

                key = tuple(vals)
                stats[key] += int(count)

        # convert {"a|b|c":count,...}  into [[a,b,c,count],...]

        stats = [list(k) + [v] for k, v in stats.iteritems()]
        writeData(self.combinedFile, stats, columnHeaders11)
        return stats

    def generateGraphData(self, stats=None):
        safePrint('Generating data files to %s' % self.pathGraphs)

        if stats is None:
            df = read_table(self.combinedFile, sep='\t')
        else:
            df = DataFrame(stats, columns=columnHeaders11)

        # filter type==DATA
        data = df[df['type'] == 'DATA']
        # filter out last date
        lastDate = data.date.max()
        data = data[data.date < lastDate]
        xcs = list(data.xcs.unique())

        for id in xcs:

            s = StringIO.StringIO()
            pivot_table(data[data.xcs == id], 'count', ['date', 'iszero'], aggfunc=np.sum).to_csv(s, header=True)
            result = s.getvalue()

            # sortColumns = ['date', 'via', 'ipset', 'https', 'lang', 'subdomain', 'site', 'iszero']
            # outColumns = ['date', 'via', 'ipset', 'https', 'lang', 'subdomain', 'site', 'iszero', 'count']
            # xcsData = data[data.xcs == id].sort(columns=sortColumns)
            # result = xcsData.sort(columns=sortColumns).to_csv(columns=outColumns, index=False)

            wiki = self.getWiki()
            wiki(
                'edit',
                title='RawData:' + id,
                summary='(bot) refreshing data',
                text=result,
                token=wiki.token()
            )

            # return data
            # pt = pivot_table(data, values='count', index=['date'], columns=['xcs','subdomain'], aggfunc=np.sum).head(10)
            # writeData(os.path.join(self.pathGraphs, 'combined-errors.tsv'),
            # ifilter(lambda v: v[1] == 'ERR', stats),
            # columnHeaders11)
            # writeData(os.path.join(self.pathGraphs, 'combined-stats.tsv'),
            # ifilter(lambda v: v[1] == 'STAT', stats), columnHeaders11)
            # writeData(os.path.join(self.pathGraphs, 'combined-data.tsv'),
            # ifilter(lambda v: v[1] == 'DATA', stats), columnHeaders11)

    def run(self):
        self.processLogFiles()
        if not self.enableUpload:
            safePrint('Uploading disabled, quiting')
        elif os.path.isfile(self.combinedFile):
            safePrint('No new data, we are done')
        else:
            stats = self.combineStats()
            self.generateGraphData(stats)

    def manualRun(self):
        # prc.reformatArch()
        # prc.processLogFiles()
        # stats = self.combineStats()
        # file = r'c:\Users\user\mw\shared\zero-sms\data\weblogs\zero.tsv.log-20140808.gz'
        # prc.processLogFile(file, file + '.json')
        # prc.downloadConfigs()
        # for f in os.listdir(self.pathCache):
        # if not self.statFileRe.match(f):
        # continue
        # pth = os.path.join(self.pathCache, f)
        # writeData(pth + '.new', readData(pth, -len(columnHeaders10)), columnHeaders10)
        # os.rename(pth, pth + '.old')
        self.generateGraphData()

    def getWiki(self):
        if not self._wiki:
            self._wiki = api.wikimedia('zero', 'wikimedia', 'https')
            if self.proxy:
                self._wiki.session.proxies = {'http': 'http://%s:%d' % (self.proxy, self.proxyPort)}
            self._wiki.login(self.settings.apiUsername, self.settings.apiPassword)
        return self._wiki


if __name__ == '__main__':
    # WebLogProcessor(logDatePattern=(sys.argv[1] if len(sys.argv) > 1 else False)).manualRun()
    WebLogProcessor(logDatePattern=(sys.argv[1] if len(sys.argv) > 1 else False)).safeRun()
