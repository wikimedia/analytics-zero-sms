# coding=utf-8
import StringIO
import re
import collections
import subprocess
from time import strftime
from calendar import monthrange

from datetime import timedelta
from dateutil.relativedelta import relativedelta

from pandas import read_table, pivot_table, DataFrame, MultiIndex
# from pandas.core.frame import DataFrame
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


def daily(date, i=False):
    """
    :param date: date to trim to the beginning of the period
    :return: if i, previous period, this period, next period; otherwise - date, period length, index in period
    """
    dt = date
    if i:
        return dt - timedelta(1), dt, dt + timedelta(1)
    return dt, 1, 0


def weekly(date, i=False):
    """
    :param date: date to trim to the beginning of the period
    :return: if i, previous period, this period, next period; otherwise - date, period length, index in period
    """
    dt = date - timedelta(date.weekday())
    if i:
        return dt - timedelta(7), dt, dt + timedelta(7)
    return dt, 7, date.weekday()


def monthly(date, i=False):
    """
    :param date: date to trim to the beginning of the period
    :return: if i, previous period, this period, next period; otherwise - date, period length, index in period
    """
    dt = date - timedelta(date.day - 1)
    if i:
        return dt - relativedelta(months=1), dt, dt + relativedelta(months=1)
    return dt, monthrange(dt.year, dt.month)[1], date.day - 1


def getHeaders(data):
    # Adapted from pandas.core.format._helper_csv
    if isinstance(data.index, MultiIndex):
        headerFields = [v for (k, v) in enumerate(data.index.names)]
        headerFields.append(data.name)
    else:
        headerFields = list(data.columns)
    return ['' if v is None else v for v in headerFields]


def insertMissingVals(lines, dateFunc):
    lines.sort()
    stats = {}
    lastDate = None
    prev, dt, nxt = None, None, None
    extraLines = []
    # Insert two zero values for each category for each xcs - one for the earliest day of the xcs data,
    # and one - in the period preceding first available data point of that series
    firstDate = {}
    for line in lines:
        date = line[0]
        if line[1] not in firstDate:
            firstDate[line[1]] = date
        if lastDate != date:
            prev, dt, nxt = dateFunc(date, True)
            lastDate = date
        key = tuple(line[1:-1])
        if key not in stats:
            stats[key] = dt
            firstDt = firstDate[line[1]]
            if firstDt < date:
                extraLines.append([firstDt] + list(key) + ['0'])
                if firstDt != prev:
                    extraLines.append([prev] + list(key) + ['0'])
        else:
            lastDt = stats[key]
            if lastDt < prev:
                # Add an entry at the beginning of the gap
                tmp = dateFunc(lastDt, True)[2]
                extraLines.append([tmp] + list(key) + ['0'])
                # Add an entry at the end of the gap
                if tmp != prev:
                    extraLines.append([prev] + list(key) + ['0'])
            stats[key] = dt

    # Insert trailing 0s in case we haven't seen them since
    # We probably don't need them because line is not extrapolated after the last data point
    # for key, dt in stats.iteritems():
    #     tmp = dateFunc(dt, True)[2]
    #     if tmp < lastDate:
    #         extraLines.append([tmp] + list(key) + ['0'])

    return lines + extraLines


class WebLogProcessor2(LogProcessor):
    def __init__(self, settingsFile):
        print('Using settings %s' % settingsFile)
        super(WebLogProcessor2, self).__init__(settingsFile, 'web2')

        self._configs = None
        self.dateDirRe = re.compile(r'^date=(\d\d\d\d-\d\d-\d\d)$')
        self.fileRe = re.compile(r'^\d+')
        self.combinedFile = os.path.join(self.pathCache, 'combined-all.tsv')
        self.allowEdit = True

    def defaultSettings(self, suffix):
        s = super(WebLogProcessor2, self).defaultSettings(suffix)
        s.checkAfterTs = False
        s.hiveTable = 'wmf_raw.webrequest'
        s.dstTable = 'zero_webstats'
        s.hqlScript = 'zero-counts.hql'
        s.wikiPageSuffix = ''
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

        if self.settings.hiveTable == 'wmf_raw.webrequest':
            pathFunc = lambda dt:\
                '/mnt/hdfs/wmf/data/raw/webrequest/webrequest_upload/hourly/%s/23' \
                % strftime("%Y/%m/%d", date.timetuple())
        elif self.settings.hiveTable == 'wmf.webrequest':
            pathFunc = lambda dt: \
                '/mnt/hdfs/wmf/data/wmf/webrequest/webrequest_source=mobile/year=%s/month=%s/day=%s/hour=23' \
                % (date.year, date.month, date.day)
        else:
            raise 'Unknown hiveTable = ' + str(self.settings.hiveTable)

        for date in dateRange(self.settings.checkAfterTs, datetime.today()):
            path = pathFunc(date)
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
                   '-f', self.settings.hqlScript,
                   '-S',  # --silent
                   '-d', 'table=' + self.settings.hiveTable,
                   '-d', 'dsttable=' + self.settings.dstTable,
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

        launchedDates = dict([(k, self.parseDate(v, self.dateFormat)) for k, v in launchedOn.iteritems()])
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

    def generateGraphData(self):
        safePrint('Generating and uploading data files')

        allData = read_table(self.combinedFile, sep='\t', na_filter=False, parse_dates=[0], infer_datetime_format=True)
        xcsList = [xcs for xcs in allData.xcs.unique() if xcs != 'ERROR' and xcs[0:4] != 'TEST' and xcs != '000-00']

        # filter type==DATA and site==wikipedia
        allData = allData[(allData['xcs'].isin(xcsList)) & (allData['site'] == 'wikipedia')]

        # By "iszero+via", e.g.  a,b,aO,bO,..., where 'a' == zero-rated, 'b' == non-zero-rated, and 'O' == Opera
        data = DataFrame(pivot_table(allData, 'count', ['date', 'xcs', 'via', 'iszero'], aggfunc=np.sum))
        data.reset_index(inplace=True)
        data['via'] = data.apply(lambda r: ('a' if r['iszero'][:1] == 'y' else 'b') + r['via'][:1], axis=1)
        data.drop('iszero', axis=1, inplace=True)
        self.createClippedData('RawData:YearDailyViaIsZero', data)
        self.createPeriodData('RawData:WeeklyViaIsZero', data, weekly)
        self.createPeriodData('RawData:MonthlyViaIsZero', data, monthly)

        allowedSubdomains = ['m', 'zero']
        data = allData[(allData.ison == 'y') & (allData.iszero == 'y') & (allData.subdomain.isin(allowedSubdomains))]
        data = DataFrame(pivot_table(data, 'count', ['date', 'xcs', 'subdomain'], aggfunc=np.sum))
        data.reset_index(inplace=True)

        self.createClippedData('RawData:YearDailySubdomains', data)
        self.createPeriodData('RawData:WeeklySubdomains', data, weekly)
        self.createPeriodData('RawData:MonthlySubdomains', data, monthly)

        # create an artificial yes/no/opera sums
        opera = allData[(allData.via == 'OPERA') & (allData.iszero == 'y')]
        opera['str'] = 'o'
        yes = allData[allData.iszero == 'y']
        yes['str'] = 'y'
        no = allData[allData.iszero == 'n']
        no['str'] = 'n'
        combined = opera.append(yes).append(no)
        data = DataFrame(pivot_table(combined, 'count', ['date', 'xcs', 'str'], aggfunc=np.sum))
        data.reset_index(inplace=True)

        headerFields = 'date,xcs,iszero,count'  # Override "str" as "iszero"
        self.createClippedData('RawData:YearDailyTotals', data, headerFields)
        self.createPeriodData('RawData:MonthlyTotals', data, monthly, headerFields)

        data = []
        for xcsId in list(allData.xcs.unique()):
            byLang = pivot_table(allData[allData.xcs == xcsId], 'count', ['lang'], aggfunc=np.sum) \
                .order('count', ascending=False)
            top = byLang.head(5)
            vals = list(top.iteritems())
            vals.append(('other', byLang.sum() - top.sum()))
            valsTotal = sum([v[1] for v in vals]) / 100.0
            data.extend(['%s,%s,%.1f' % (l, xcsId, c / valsTotal) for l, c in vals])

        self.saveWikiPage('RawData:LangPercent', data, 'lang,xcs,count')

    def createClippedData(self, wikiTitle, data, headerFields=False):
        """
        Convert daily data to monthly data
        :type wikiTitle: string|string[]
        :type data: mixed
        :type headerFields: string|bool
        """
        if not headerFields:
            headerFields = getHeaders(data)

        minDate = datetime.today() - relativedelta(years=1)
        clipped = data[data['date'] > minDate]

        lines = [list(row) for _, row in clipped.iterrows()]
        lines = insertMissingVals(lines, daily)
        self.saveWikiPage(wikiTitle, lines, headerFields)

    def createPeriodData(self, wikiTitle, data, dateFunc, headerFields=False):
        """
        Convert daily data to periodic data
        :type wikiTitle: string|string[]
        :type data: mixed
        :type headerFields: string|bool
        """
        if not headerFields:
            headerFields = getHeaders(data)

        # Map of key (date,xcs,XXX): [list counts or '', one for each day]
        stats = {}
        # Short key (date,xcs): [list of true/false, one for each day]
        goodDays = {}
        for _, rowData in data.iterrows():
            parts = list(rowData)
            valueDate = parts[0]
            valueKey = parts[:-1]
            valueCount = parts[-1]
            thisId, periodLen, periodInd = dateFunc(valueDate)
            valueKey[0] = thisId
            valueKey = tuple(valueKey)

            if valueKey in stats:
                vals = stats[valueKey]
            else:
                # new list filled with ''s, one value for each day in the month
                vals = [''] * periodLen
                stats[valueKey] = vals
            if vals[periodInd] != '':
                raise IndexError('Duplicate key %s. Existing value %d' % (vals, vals[periodInd]))
            vals[periodInd] = valueCount

            # Create a list, one for each day of the month, True if any key exists for that day
            key = tuple(valueKey[:-1])  # without the last key part
            if key in goodDays:
                vals = goodDays[key]
            else:
                # new list filled with False, one value for each day in the month
                vals = [False] * periodLen
                goodDays[key] = vals
            vals[periodInd] = True

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
            lines.append(list(k) + [str(total)])

        lines = insertMissingVals(lines, dateFunc)
        self.saveWikiPage(wikiTitle, lines, headerFields)

    def saveWikiPage(self, wikiTitle, data, headerFields=False):
        """
        :type wikiTitle: string|string[]
        :type data: mixed
        :type headerFields: string|bool
        """
        if isinstance(data, list):
            if len(data) == 0:
                text = ''
            else:
                if isinstance(data[0], list) and isinstance(data[0][0], datetime):
                    lines = []
                    for line in data:
                        line[0] = line[0].strftime('%Y-%m-%d')
                        line[-1] = str(line[-1])
                        lines.append(','.join(line))
                    lines.sort()
                else:
                    lines = data
                text = '\n'.join(lines)
        elif hasattr(data, 'to_csv'):
            s = StringIO.StringIO()
            if headerFields:
                includeHeaders = False
            else:
                includeHeaders = True
            data.to_csv(s, header=includeHeaders, index=False, date_format='%Y-%m-%d')
            text = s.getvalue()
        else:
            raise Exception('Unknown data type ' + str(type(data)))

        if headerFields:
            if isinstance(headerFields, list):
                headerFields = ','.join(headerFields)
            text = '{0}\n{1}'.format(headerFields, text)

        if self.allowEdit:
            wiki = self.getWiki()
            wiki(
                'edit',
                title=wikiTitle + self.settings.wikiPageSuffix,
                summary='refreshing data',
                text=text,
                token=wiki.token()
            )

    def run(self):
        self.runHql()
        self.combineStats()
        self.generateGraphData()

    def manualRun(self):
        # self.allowEdit = False
        # self.runHql()
        # w = self.getWiki()
        # w.noSSL = True
        # self.combineStats()
        # self.generateGraphData()
        pass


if __name__ == '__main__':
    # WebLogProcessor2('settings/weblogs2.local.json').manualRun()
    import sys

    WebLogProcessor2('settings/weblogs2.json' if len(sys.argv) < 2 else sys.argv[1]).safeRun()
