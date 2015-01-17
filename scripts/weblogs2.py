# coding=utf-8
import StringIO
import re
import collections
import subprocess
from time import strftime
from calendar import monthrange

from datetime import timedelta
from pandas import read_table, pivot_table
from pandas.core.frame import DataFrame
import numpy as np

from logprocessor import *

columnHdrCache = u'xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHdrResult = u'date,xcs,via,ipset,https,lang,subdomain,site,iszero,ison,count'.split(',')

launchedOn = {
    # '123-45': '2006-03-01',
}


def dateRange(start, before):
    for n in xrange(int((before - start).days)):
        yield start + timedelta(n)


class WebLogProcessor2(LogProcessor):
    def __init__(self, settingsFile='settings/weblogs2.json'):
        super(WebLogProcessor2, self).__init__(settingsFile, 'web2')

        self._configs = None
        self.dateDirRe = re.compile(r'^date=(\d\d\d\d-\d\d-\d\d)$')
        self.fileRe = re.compile(r'^\d+')
        self.combinedFile = os.path.join(self.pathCache, 'combined-all.tsv')
        self.allowEdit = True

    def defaultSettings(self, suffix):
        s = super(WebLogProcessor2, self).defaultSettings(suffix)
        s.checkAfterTs = False
        return s

    def onSavingSettings(self):
        super(WebLogProcessor2, self).onSavingSettings()
        s = self.settings
        s.checkAfterTs = self.formatDate(s.checkAfterTs, self.dateFormat)

    def onSettingsLoaded(self):
        super(WebLogProcessor2, self).onSettingsLoaded()
        s = self.settings
        s.checkAfterTs = self.parseDate(s.checkAfterTs, self.dateFormat)
        if not s.checkAfterTs:
            s.checkAfterTs = self.parseDate('2015-01-01', self.dateFormat)

    def runHql(self):
        os.environ["HADOOP_HEAPSIZE"] = "2048"

        for date in dateRange(self.settings.checkAfterTs, datetime.today()):
            path = '/mnt/hdfs/wmf/data/raw/webrequest/webrequest_upload/hourly/%s/23' \
                   % strftime("%Y/%m/%d", date.timetuple())
            if not os.path.exists(path):
                continue
            size = sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path) if
                       os.path.isfile(os.path.join(path, f)))
            if size < 50000:
                print('***** "%s" is %d bytes -- too small' % (path, size))
                continue

            path = os.path.join(self.settings.pathLogs, 'date=%s' % strftime("%Y-%m-%d", date.timetuple()))
            if os.path.exists(path):
                continue

            cmd = ['hive',
                   '-f', 'zero-counts.hql',
                   '-S',  # --silent
                   '-d', 'table=wmf_raw.webrequest',
                   '-d', 'year=' + strftime("%Y", date.timetuple()),
                   '-d', 'month=' + strftime("%m", date.timetuple()),
                   '-d', 'day=' + strftime("%d", date.timetuple()),
                   '-d', 'date=' + strftime("%Y-%m-%d", date.timetuple())]

            print('Running HQl: %s' % ' '.join(cmd))

            ret = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            print(ret)
            self.settings.checkAfterTs = date

    def downloadConfigs(self):
        if self._configs:
            return self._configs

        launchedDates = dict([(k,self.parseDate(v, self.dateFormat)) for k,v in launchedOn.iteritems()])
        # If the very first config value starts on a date between these, treat all data as enabled
        importStart = self.parseDate('2014-04-01', self.dateFormat)
        importEnd = self.parseDate('2014-05-01', self.dateFormat)
        defaultLaunched = self.parseDate('2000-01-01', self.dateFormat)
        # If 'via' is opera only, up to and including this day add 'direct'
        directProxyFixDate = self.parseDate('2014-07-03', self.dateFormat)

        wiki = self.getWiki()
        # https://zero.wikimedia.org/w/api.php?action=zeroportal&type=analyticsconfig&format=jsonfm
        configs = wiki('zeroportal', type='analyticsconfig').zeroportal
        for xcs, items in configs.iteritems():
            isFirst = True
            launched = launchedDates[xcs] if xcs in launchedDates else defaultLaunched
            for c in items:
                c.frm = datetime.strptime(c['from'], '%Y-%m-%dT%H:%M:%SZ')
                if c.before is None:
                    c.before = datetime.max
                else:
                    c.before = datetime.strptime(c.before, '%Y-%m-%dT%H:%M:%SZ')
                c.languages = True if True == c.languages else set(c.languages)
                c.sites = True if True == c.sites else set(c.sites)
                c.via = set(c.via)
                # Before this date 'DIRECT' was not explicit
                if c.before <= directProxyFixDate and c.via == {'OPERA'}:
                    c.via.add('DIRECT')
                c.ipsets = set(c.ipsets)
                c.enabled = 'enabled' not in c or c.enabled
                # configs were created in april 2014, but zero has been running longer than that
                # adjust configs' starting date if needed
                if c.enabled:
                    if c.frm < defaultLaunched:
                        if c.before < defaultLaunched:
                            c.enabled = False
                        else:
                            c.frm = defaultLaunched
                    elif isFirst and importStart <= c.frm < importEnd:
                        c.frm = launched
                isFirst = False
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
                            if conf.enabled and conf.frm <= dt < conf.before:
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

    def createMonthlyData(self, totals, headerFields, wikiTitle):
        """
        Convert daily totals to monthly totals
        :param totals:
        :param headerFields:
        :param wikiTitle:
        :return:
        """

        # Map of key (month-day,xcs,XXX): [list counts of '', one for each day]
        stats = {}
        # Short key (month-date,xcs): [list of true/false, one for each day]
        goodDays = {}
        for k, v in totals.iteritems():
            date = k[0].split('-')
            day = int(date[2]) - 1
            key = tuple((date[0] + '-' + date[1] + '-01',) + k[1:])
            if key in stats:
                vals = stats[key]
            else:
                # new list filled with ''s, one value for each day in the month
                vals = [''] * monthrange(int(date[0]), int(date[1]))[1]
                stats[key] = vals
            if vals[day] != '':
                raise IndexError('Duplicate key %s. Existing value %d' % (k, vals[day]))
            vals[day] = v

            # Create a list, one for each day of the month, True if any key exists for that day
            key = tuple((date[0] + '-' + date[1] + '-01',) + k[1:-1])  # without the last key part
            if key in goodDays:
                vals = goodDays[key]
            else:
                # new list filled with False, one value for each day in the month
                vals = [False] * monthrange(int(date[0]), int(date[1]))[1]
                goodDays[key] = vals
            vals[day] = True

        lines = []
        for k, v in stats.iteritems():
            # Replace '' with 0 for any day that had values for other keys for the same carrier
            goodDay = goodDays[tuple(k[:-1])]
            for i in xrange(len(v)):
                if v[i] == '' and goodDay[i]:
                    v[i] = 0

            # v is now a list of integers, one for each day of the month
            # first, remove any value had a missing '' value either before or after it
            count = 0
            total = 0
            for i in xrange(len(v)):
                if v[i] != '' and (i == 0 or v[i - 1] != '') and (i == len(v) - 1 or v[i + 1] != ''):
                    total += v[i]
                    count += 1
            if count == 0:
                # if too much data is missing, average whatever is available
                for i in xrange(len(v)):
                    if v[i] != '':
                        total += v[i]
                        count += 1
            # Use monthly average for all missing/uncounted days when calculating monthly total
            total += int((float(total) / count) * (len(v) - count))
            lines.append(','.join(k) + ',' + str(total))

        lines.sort()

        if self.allowEdit:
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

        headerFields = 'date,xcs,subdomain,count'

        s = StringIO.StringIO()
        allowedSubdomains = ['m', 'zero']
        dailySubdomains = df[(df.ison == 'y') & (df.iszero == 'y') & (df.subdomain.isin(allowedSubdomains))]
        totals = pivot_table(dailySubdomains, 'count', ['date', 'xcs', 'subdomain'], aggfunc=np.sum)
        totals.to_csv(s, header=False)

        if self.allowEdit:
            wiki(
                'edit',
                title='RawData:DailySubdomains',
                summary='refreshing data',
                text=headerFields + '\n' + s.getvalue(),
                token=wiki.token()
            )

        self.createMonthlyData(totals, headerFields, 'RawData:MonthlySubdomains')

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
        totals = pivot_table(combined, 'count', ['date', 'xcs', 'str'], aggfunc=np.sum)
        totals.to_csv(s, header=False)

        if self.allowEdit:
            wiki(
                'edit',
                title='RawData:DailyTotals',
                summary='refreshing data',
                text=headerFields + '\n' + s.getvalue(),
                token=wiki.token()
            )

        self.createMonthlyData(totals, headerFields, 'RawData:MonthlyTotals')

        results = ['lang,xcs,count']
        for xcsId in list(df.xcs.unique()):
            byLang = pivot_table(df[df.xcs == xcsId], 'count', ['lang'], aggfunc=np.sum).order('count', ascending=False)
            top = byLang.head(5)
            vals = list(top.iteritems())
            vals.append(('other', byLang.sum() - top.sum()))
            valsTotal = sum([v[1] for v in vals]) / 100.0
            results.extend(['%s,%s,%.1f' % (l, xcsId, c / valsTotal) for l, c in vals])

        if self.allowEdit:
            wiki(
                'edit',
                title='RawData:LangPercent',
                summary='refreshing data',
                text='\n'.join(results),
                token=wiki.token()
            )

    def run(self):
        self.runHql()
        stats = self.combineStats()
        self.generateGraphData(stats)

    def manualRun(self):
        self.allowEdit = False
        # self.runHql()
        stats = False
        # stats = self.combineStats()
        self.generateGraphData(stats)


if __name__ == '__main__':
    # WebLogProcessor2().manualRun()
    WebLogProcessor2().safeRun()
