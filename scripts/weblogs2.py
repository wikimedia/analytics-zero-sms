# coding=utf-8
import StringIO
import re
import collections

from pandas import read_table, pivot_table
from pandas.core.frame import DataFrame, Series
import numpy as np

from logprocessor import *


def addStat(stats, date, dataType, xcs, via, ipset, https, lang, subdomain, site):
    key = (date, dataType, xcs, via, ipset, 'https' if https else 'http', lang, subdomain, site)
    if key in stats:
        stats[key] += 1
    else:
        datetime.strptime(date, '%Y-%m-%d')  # Validate date - slow operation, do it only once per key
        stats[key] = 1


columnHdrCache = u'xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHdrCacheLegacy = u'date,type,xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHdrResult = u'date,type,xcs,via,ipset,https,lang,subdomain,site,iszero,ison,count'.split(',')
validSubDomains = {'m', 'zero', 'mobile', 'wap'}
validHttpCode = {'200', '304'}


class WebLogProcessor2(LogProcessor):
    def __init__(self, settingsFile='settings/weblogs2.json'):
        super(WebLogProcessor2, self).__init__(settingsFile, 'web2')

        self._configs = None
        self.dateDirRe = re.compile(r'^date=(\d\d\d\d-\d\d-\d\d)$')
        self.fileRe = re.compile(r'^\d+')
        self.combinedFile = os.path.join(self.pathGraphs, 'combined-all.tsv')
        if self.settings.pathCacheLegacy:
            self.pathCacheLegacy = self.normalizePath(self.settings.pathCacheLegacy)
        else:
            self.pathCacheLegacy = False

        self.legacyFileRe = re.compile(r'^(zero\.tsv\.log-(\d+)\.gz)__\d+\.tsv$', re.IGNORECASE)

    def defaultSettings(self, suffix):
        s = super(WebLogProcessor2, self).defaultSettings(suffix)
        s.pathCacheLegacy = False
        return s

    def downloadConfigs(self):
        if self._configs:
            return self._configs
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
        self._configs = configs
        return self._configs

    def combineStatsLegacy(self):
        if not self.pathCacheLegacy:
            return {}
        safePrint('Combine legacy stat files')
        # Logs did not contain the "VIA" X-Analytics tag before this date
        ignoreViaBefore = datetime(2014, 3, 22)
        configs = self.downloadConfigs()
        stats = collections.defaultdict(int)
        for f in os.listdir(self.pathCacheLegacy):
            if not self.legacyFileRe.match(f):
                continue
            for vals in readData(os.path.join(self.pathCacheLegacy, f), columnHdrCacheLegacy):
                # "0          1    2      3      4       5    6  7    8         9"
                # "2014-07-25 DATA 250-99 DIRECT default http ru zero wikipedia 1000"
                if len(vals) != 10:
                    if len(vals) == 11 and vals[3] == '':
                        safePrint('Fixing extra empty xcs in file %s' % f)
                        del vals[3]
                    else:
                        raise ValueError('Unrecognized key (%s) in file %s' % (joinValues(vals), f))
                (dt, typ, xcs, via, ipset, https, lang, subdomain, site, count) = vals

                via = via.upper()

                error = False
                if xcs == '404-01b':
                    vals[2] = xcs = '404-01'
                    vals[4] = ipset = 'b'
                if typ == 'DATA' and site not in validSites:
                    error = 'bad-site'
                elif xcs in configs:
                    isZero = ''
                    isOn = ''
                    if typ == 'DATA':
                        dt = datetime.strptime(dt, '%Y-%m-%d')
                        site2 = subdomain + '.' + site
                        isZero = False
                        isEnabled = False
                        for conf in configs[xcs]:
                            langs = conf.languages
                            sites = conf.sites
                            if conf['from'] <= dt < conf.before:
                                if 'enabled' not in conf or conf.enabled:
                                    isEnabled = True
                                if (conf.https or https == u'http') and \
                                        (True == langs or lang in langs) and \
                                        (True == sites or site2 in sites) and \
                                        (dt < ignoreViaBefore or via in conf.via) and \
                                        (ipset in conf.ipsets):
                                    isZero = True
                                    break
                        isZero = u'yes' if isZero else u'no'
                        isOn = u'on' if isEnabled else u'off'

                    vals[9] = isZero
                    vals.append(isOn)
                else:
                    # X-CS does not exist, ignore it
                    error = 'xcs'

                if error:
                    vals = (vals[0], 'ERR', '000-00', 'ERR', 'ERR', 'http', '', error, '', '', '')

                key = tuple(vals)
                stats[key] += int(count)

        return stats

    def combineStats(self, legacyStats=None):
        safePrint('Combine stat files')
        # Logs did not contain the "VIA" X-Analytics tag before this date
        ignoreViaBefore = datetime(2014, 3, 22)
        configs = self.downloadConfigs()
        stats = collections.defaultdict(int)
        for dateDir in os.listdir(self.pathCache):
            m = self.dateDirRe.match(dateDir)
            if not m:
                continue
            dateStr = m.group(1)
            dt = datetime.strptime(dateStr, '%Y-%m-%d')
            datePath = os.path.join(self.pathCache, dateDir)
            for f in os.listdir(datePath):
                if not self.fileRe.match(f):
                    continue
                for vals in readData(os.path.join(datePath, f), -len(columnHdrCache)):
                    # 0      1      2       3    4  5    6         7
                    # 250-99 DIRECT default http ru zero wikipedia 1000
                    (xcs, via, ipset, https, lang, subdomain, site, count) = vals

                    via = via.upper() if via else u'DIRECT'
                    ipset = ipset if ipset else u'default'
                    https = https if https else u'http'

                    error = False

                    if xcs == '404-01b':
                        vals[2] = xcs = '404-01'
                        vals[4] = ipset = 'b'

                    if site not in validSites:
                        error = 'bad-site'
                    elif xcs in configs:
                        site2 = subdomain + '.' + site
                        isZero = False
                        isEnabled = False
                        for conf in configs[xcs]:
                            langs = conf.languages
                            sites = conf.sites
                            if conf['from'] <= dt < conf.before:
                                if 'enabled' not in conf or conf.enabled:
                                    isEnabled = True
                                if (conf.https or https == u'http') and \
                                        (True == langs or lang in langs) and \
                                        (True == sites or site2 in sites) and \
                                        (dt < ignoreViaBefore or via in conf.via) and \
                                        (ipset in conf.ipsets):
                                    isZero = True
                                    break
                        isZero = u'yes' if isZero else u'no'
                        isOn = u'on' if isEnabled else u'off'

                        vals = (dateStr, 'DATA', xcs, via, ipset, https, lang, subdomain, site, isZero, isOn)
                    else:
                        # X-CS does not exist, ignore it
                        error = 'xcs'

                    if error:
                        vals = (dateStr, 'ERR', '000-00', 'ERR', 'ERR', 'http', '', error, '', '', '')

                    stats[vals] += int(count)

        if legacyStats:
            # Only add legacy data for dates that we haven't seen in hadoop
            earliest = min([k[0] for k in stats.keys()])
            for k, v in legacyStats.iteritems():
                if k[0] < earliest:
                    stats[k] = v

        stats = [list(k) + [v] for k, v in stats.iteritems()]
        writeData(self.combinedFile, stats, columnHdrResult)
        return stats

    def generateGraphData(self, stats=None):
        safePrint('Generating data files to %s' % self.pathGraphs)

        wiki = self.getWiki()

        if stats is None:
            allData = read_table(self.combinedFile, sep='\t')
        else:
            allData = DataFrame(stats, columns=columnHdrResult)

        # filter type==DATA and site==wikipedia
        allData = allData[(allData['type'] == 'DATA') & (allData['site'] == 'wikipedia')]
        # filter out last date
        lastDate = allData.date.max()
        df = allData[allData.date < lastDate]

        allEnabled = df[(df.ison == 'on') & (df.iszero == 'yes')]
        s = StringIO.StringIO()
        pivot_table(allEnabled, 'count', ['date', 'xcs'], aggfunc=np.sum).to_csv(s, header=True)
        result = s.getvalue()

        wiki(
            'edit',
            title='RawData:AllEnabled',
            summary='refreshing data',
            text=result,
            token=wiki.token()
        )

        xcsList = list(df.xcs.unique())
        xcsList.sort()
        for id in xcsList:
            xcsDf = df[df.xcs == id]

            # create an artificial yes/opera value
            opera = xcsDf[(xcsDf.via == 'OPERA') & (xcsDf.iszero == 'yes')]
            opera['str'] = 'zero-opera'

            yes = xcsDf[xcsDf.iszero == 'yes']
            yes['str'] = 'zero-all'

            no = xcsDf[xcsDf.iszero == 'no']
            no['str'] = 'non-zero'

            combined = opera.append(yes).append(no)

            s = StringIO.StringIO()
            pivot_table(combined, 'count', ['date', 'str'], aggfunc=np.sum).to_csv(s, header=False)
            result = 'date,iszero,count\n' + s.getvalue()

            wiki(
                'edit',
                title='RawData:' + id,
                summary='refreshing data',
                text=result,
                token=wiki.token()
            )

            byLang = pivot_table(xcsDf, 'count', ['lang'], aggfunc=np.sum).order('count', ascending=False)
            top = byLang.head(5)
            other = byLang.sum() - top.sum()
            s = StringIO.StringIO()
            Series.to_csv(top, s)
            result = 'lang,count\n' + s.getvalue() + ('other,%d\n' % other)

            wiki(
                'edit',
                title='RawData:' + id + '-langTotal',
                summary='refreshing data',
                text=result,
                token=wiki.token()
            )

            # return df
            # pt = pivot_table(df, values='count', index=['date'], columns=['xcs','subdomain'], aggfunc=np.sum).head(10)
            # writeData(os.path.join(self.pathGraphs, 'combined-errors.tsv'),
            # ifilter(lambda v: v[1] == 'ERR', stats),
            # columnHeaders11)
            # writeData(os.path.join(self.pathGraphs, 'combined-stats.tsv'),
            # ifilter(lambda v: v[1] == 'STAT', stats), columnHeaders11)
            # writeData(os.path.join(self.pathGraphs, 'combined-data.tsv'),
            # ifilter(lambda v: v[1] == 'DATA', stats), columnHeaders11)

    def run(self):
        stats = self.combineStats(self.combineStatsLegacy())
        self.generateGraphData(stats)

    def manualRun(self):
        stats = self.combineStats()
        # self.generateGraphData(stats)


if __name__ == '__main__':
    # WebLogProcessor2().manualRun()
    WebLogProcessor2().safeRun()
