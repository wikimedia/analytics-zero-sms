# coding=utf-8
import StringIO
import re
import collections

from pandas import read_table, pivot_table, Series
from pandas.core.frame import DataFrame
import numpy as np
from calendar import monthrange

from logprocessor import *


def addStat(stats, date, dataType, xcs, via, ipset, https, lang, subdomain, site):
    key = (date, dataType, xcs, via, ipset, 'https' if https else 'http', lang, subdomain, site)
    if key in stats:
        stats[key] += 1
    else:
        datetime.strptime(date, '%Y-%m-%d')  # Validate date - slow operation, do it only once per key
        stats[key] = 1


columnHdrCache = u'xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHdrResult = u'date,xcs,via,ipset,https,lang,subdomain,site,iszero,ison,count'.split(',')


def toYearMonth(dateStr):
    parts = dateStr.split('-')
    return parts[0] + '-' + parts[1] + '-01'


class WebLogProcessor2(LogProcessor):
    def __init__(self, settingsFile='settings/weblogs2.json'):
        super(WebLogProcessor2, self).__init__(settingsFile, 'web2')

        self._configs = None
        self.dateDirRe = re.compile(r'^date=(\d\d\d\d-\d\d-\d\d)$')
        self.fileRe = re.compile(r'^\d+')
        self.combinedFile = os.path.join(self.pathCache, 'combined-all.tsv')

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

    def combineStats(self):
        safePrint('Loading hadoop files')
        # Logs did not contain the "VIA" X-Analytics tag before this date
        ignoreViaBefore = datetime(2014, 3, 22)
        configs = self.downloadConfigs()
        stats = collections.defaultdict(int)
        for dateDir in os.listdir(self.pathLogs):
            m = self.dateDirRe.match(dateDir)
            if not m:
                continue
            dateStr = m.group(1)
            dt = datetime.strptime(dateStr, '%Y-%m-%d')
            datePath = os.path.join(self.pathLogs, dateDir)
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

                        via = u'' if via == 'DIRECT' else (u'NOKIA' if via == 'NOKIAPROD' else via)
                        ipset = u'' if ipset == 'default' else ipset
                        https = u'' if https == 'http' else 's'
                        isZero = u'y' if isZero else u'n'
                        isOn = u'y' if isEnabled else u'n'

                        vals = (dateStr, xcs, via, ipset, https, lang, subdomain, site, isZero, isOn)
                    else:
                        # X-CS does not exist, ignore it
                        error = 'xcs'

                    if error:
                        vals = (dateStr, 'ERROR', 'ERR', 'ERR', 'http', '', error, '', '', '')

                    stats[vals] += int(count)

        stats = [list(k) + [v] for k, v in stats.iteritems()]
        writeData(self.combinedFile, stats, columnHdrResult)
        return stats

    def createMonthlyData(self, totals, pivotFields, headerFields, wikiTitle):
        """
        Convert daily totals to monthly totals
        :param totals:
        :param pivotFields:
        :param headerFields:
        :param wikiTitle:
        :return:
        """
        monthly = totals.reset_index()
        # remove last month if there are less than 10 days in it
        dates = monthly['date'].unique()
        lastMonth = dates.max().split('-')
        lastMonth = lastMonth[0] + '-' + lastMonth[1] + '-00'
        daysInLastMonth = dates[dates > lastMonth].size
        if daysInLastMonth < 10:
            monthly = monthly[monthly.date < lastMonth]
        monthly['date'] = map(toYearMonth, monthly['date'])
        monthlyTotals = pivot_table(monthly, 'count', pivotFields, aggfunc=np.sum)

        lines = []
        for k, v in monthlyTotals.to_dict().iteritems():
            count = v
            if k[0] < lastMonth:
                parts = k[0].split('-')
                dayCount = monthrange(int(parts[0]), int(parts[1]))[1]
            else:
                dayCount = daysInLastMonth
            vals = list(k)
            vals.append(str(int(count * 30.0 / dayCount)))
            lines.append(','.join(vals))

        lines.sort()
        wiki = self.getWiki()
        wiki(
            'edit',
            title=wikiTitle,
            summary='refreshing data',
            text=headerFields + '\n' + '\n'.join(lines),
            token=wiki.token()
        )

    def generateGraphData(self, stats=None):
        safePrint('Generating and uploading data files')

        wiki = self.getWiki()

        if not stats:
            allData = read_table(self.combinedFile, sep='\t', na_filter=False)
        else:
            allData = DataFrame(stats, columns=columnHdrResult)

        xcsList = [xcs for xcs in allData.xcs.unique() if xcs != 'ERROR' and xcs[0:4] != 'TEST']

        # filter type==DATA and site==wikipedia
        df = allData[(allData['xcs'].isin(xcsList)) & (allData['site'] == 'wikipedia')]

        pivotFields = ['date', 'xcs', 'subdomain']
        headerFields = 'date,xcs,subdomain,count'

        s = StringIO.StringIO()
        allowedSubdomains = ['m', 'zero']
        dailySubdomains = df[(df.ison == 'y') & (df.iszero == 'y') & (df.subdomain.isin(allowedSubdomains))]
        totals = pivot_table(dailySubdomains, 'count', pivotFields, aggfunc=np.sum)
        totals.to_csv(s, header=False)

        wiki(
            'edit',
            title='RawData:DailySubdomains',
            summary='refreshing data',
            text=headerFields + '\n' + s.getvalue(),
            token=wiki.token()
        )

        self.createMonthlyData(totals, pivotFields, headerFields, 'RawData:MonthlySubdomains')

        pivotFields = ['date', 'xcs', 'str']
        headerFields = 'date,xcs,iszero,count'
        # create an artificial yes/no/opera sums
        opera = df[(df.via == 'OPERA') & (df.iszero == 'y')]
        opera['str'] = 'o'
        yes = df[df.iszero == 'y']
        yes['str'] = 'y'
        no = df[df.iszero == 'n']
        no['str'] = 'n'
        combined = opera.append(yes).append(no)
        s = StringIO.StringIO()
        totals = pivot_table(combined, 'count', pivotFields, aggfunc=np.sum)
        totals.to_csv(s, header=False)
        wiki(
            'edit',
            title='RawData:DailyTotals',
            summary='refreshing data',
            text=headerFields + '\n' + s.getvalue(),
            token=wiki.token()
        )

        self.createMonthlyData(totals, pivotFields, headerFields, 'RawData:MonthlyTotals')

        results = ['lang,xcs,count']
        for xcsId in list(df.xcs.unique()):
            byLang = pivot_table(df[df.xcs == xcsId], 'count', ['lang'], aggfunc=np.sum).order('count', ascending=False)
            top = byLang.head(5)
            vals = list(top.iteritems())
            vals.append(('other', byLang.sum() - top.sum()))
            valsTotal = sum([v[1] for v in vals]) / 100.0
            results.extend(['%s,%s,%.1f' % (l, xcsId, c / valsTotal) for l, c in vals])

        wiki(
            'edit',
            title='RawData:LangPercent',
            summary='refreshing data',
            text='\n'.join(results),
            token=wiki.token()
        )

    def run(self):
        stats = self.combineStats()
        self.generateGraphData(stats)

    def manualRun(self):
        stats = False
        # stats = self.combineStats()
        self.generateGraphData(stats)


if __name__ == '__main__':
    # WebLogProcessor2().manualRun()
    WebLogProcessor2().safeRun()
