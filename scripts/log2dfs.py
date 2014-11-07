# coding=utf-8
import gzip
import re
import sys

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote

from logprocessor import *


columnHdrCache = u'date,type,xcs,via,ipset,https,lang,subdomain,site,count'.split(',')
columnHdrResult = u'date,type,xcs,via,ipset,https,lang,subdomain,site,iszero,ison,count'.split(',')
validSubDomains = {'m', 'zero', 'mobile', 'wap'}
validHttpCode = {'200', '304'}


def isValidInt(val):
    try:
        int(val)
        return True
    except ValueError:
        return False


xcsFromName = {
    'celcom-malaysia': '502-13',
    'dialog-sri-lanka': '413-02',
    'digi-malaysia': '502-16',
    'dtac-thailand': '520-18',
    'grameenphone-bangladesh': '470-01',
    'hello-cambodia': '456-02',
    'orange-botswana': '652-02',
    'orange-cameroon': '624-02',
    'orange-congo': '630-86',
    'orange-ivory-coast': '612-03',
    'orange-kenya': '639-07',  # Also 639-02 (Safaricom Kenya)
    'orange-morocco': '604-00',
    'orange-niger': '614-04',
    'orange-tunesia': '605-01',  # Also 639-02 (Safaricom Kenya)
    'orange-uganda': '641-14',
    'saudi-telecom': '420-01',  # Also 652-02 (orange-botswana)
    'tata-india': '405-029',
    'telenor-montenegro': '297-01',  # Also 250-99 (beeline ru)
    'tim-brasil': '724-02',
    'vodaphone-india': '404-01',
    'xl-axiata-indonesia': '510-11',
}

httpStatuses = {
    '-': '',
    'hit': 'hit',
    'miss': 'miss',
    'pass': 'pass',
    'TCP_CLIENT_REFRESH_MISS': 'miss',
    'TCP_MEM_HIT': 'hit',
    'TCP_MISS': 'miss',
    'TCP_REFRESH_HIT': 'hit',
    'FAKE_CACHE_STATUS': '',
    'TCP_HIT': 'hit',
    'TCP_IMS_HIT': 'hit',
    'TCP_DENIED': 'denied',
    'TCP_REFRESH_MISS': 'miss',
    'TCP_NEGATIVE_HIT': 'hit',
}


class LogConverter(LogProcessor):
    def __init__(self, filePattern=False, settingsFile='settings/log2dfs.json'):
        super(LogConverter, self).__init__(settingsFile, 'w2h')

        if not filePattern:
            filePattern = r'\d\d\d\d\d\d\d\d'
        self.logFileRe = re.compile(unicode(filePattern), re.IGNORECASE)
        self.dateRe = re.compile(r'(201\d-\d\d-\d\dT\d\d):\d\d:\d\d(\.\d+)?')
        self.urlRe = re.compile(r'^(https?)://([^/]+)([^?#]*)(.*)', re.IGNORECASE)
        self.xcsRe = re.compile(r'^[0-9]+-[0-9]+$', re.IGNORECASE)

    def processLogFiles(self):

        safePrint('Processing log files')
        for f in os.listdir(self.pathLogs):

            if not self.logFileRe.search(f):
                continue
            logFile = os.path.join(self.pathLogs, f)
            statFile = os.path.join(self.pathCache, f)
            if statFile.endswith('.gz'):
                statFile = statFile[:-3]

            if not os.path.exists(statFile):
                self.processLogFile(logFile, statFile)

    def processLogFile(self, logFile, statFile):
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
        count = 0
        isTab = '.tsv' in logFile or '.tab' in logFile
        isNoOpera = 'no_opera' in logFile
        webrequest_source = 'mobile'
        lastDate = year = month = day = hour = None
        xcsWarns = set()

        defaultXcs = None
        for k, v in xcsFromName.iteritems():
            if k in logFile:
                defaultXcs = u'zero=' + v
                break

        if logFile.endswith('.gz'):
            streamData = io.TextIOWrapper(io.BufferedReader(gzip.open(logFile)), encoding='utf8', errors='ignore')
        else:
            streamData = io.open(logFile, 'r', encoding='utf8', errors='ignore')

        tmpFile = statFile + '.tmp'
        with io.open(tmpFile, 'w', encoding='utf8') as out:
            for line in streamData:
                count += 1
                if count % 1000000 == 0:
                    safePrint('%d lines processed' % count)

                strip = line.strip('\n\r\x00')
                if isTab:
                    l = strip.split('\t')
                    if len(l) > 2 and l[2].startswith('201'):
                        while len(l) > 16:
                            l[13] += ' ' + l[14]
                            del l[11]
                else:
                    l = strip.split(' ')
                    # fix text/html; charset=UTF-8 into one field
                    while len(l) > 11 and l[10].endswith(';') and l[11] != '-' and not l[11].startswith('http'):
                        l[10] += ' ' + l[11]
                        del l[11]
                    if len(l) == 14:
                        l.append(u'')
                        l.append(u'')

                partsCount = len(l)
                if partsCount != 16:
                    safePrint(u'Wrong parts count - %d parts\n%s' % (partsCount, line))
                    continue

                l = ['' if v == '-' else v.replace('\t', ' ') for v in l]
                (hostname, sequence, dt, time_firstbyte, ip, status, response_size, http_method, uri, unknown1,
                 content_type, referer, x_forwarded_for, user_agent, accept_language, x_analytics) = l
                # status -> cache_status, http_status
                # uri -> uri_host, uri_path, uri_query
                # new:  webrequest_source, year, month, day, hour

                user_agent = unquote(user_agent).replace('\t', ' ')

                m = self.dateRe.match(dt)
                if not m:
                    safePrint(u'Invalid date\n%s' % line)
                    continue
                if lastDate != m.group(1):
                    lastDate = m.group(1)
                    d = datetime.strptime(lastDate, r'%Y-%m-%dT%H')
                    year = unicode(d.year)
                    month = unicode(d.month)
                    day = unicode(d.day)
                    hour = unicode(d.hour)

                if self.xcsRe.match(x_analytics):
                    x_analytics = 'zero=' + x_analytics

                if 'zero=' not in x_analytics:
                    if defaultXcs:
                        if x_analytics:
                            x_analytics += ';'
                        x_analytics += defaultXcs
                    else:
                        safePrint(u'String too short - %d parts\n%s' % (partsCount, line))
                        continue
                elif defaultXcs and x_analytics not in xcsWarns:
                    if defaultXcs not in x_analytics:
                        safePrint(u'Warning: XCS mismatch, expecting "%s", found "%s"' % (defaultXcs, x_analytics))
                    else:
                        safePrint(u'Warning: XCS confirmed, found expected "%s"' % defaultXcs)
                    xcsWarns.add(x_analytics)

                # expand "hit/200" into "hit", "200"
                tmp = status.split(u'/')
                if len(tmp) != 2 or not isValidInt(tmp[1]):
                    safePrint(u'Invalid status - "%s"\n%s' % (status, line))
                    continue
                (cache_status, http_status) = tmp
                if cache_status not in httpStatuses:
                    safePrint(u'Unknown cache_status - "%s"\n%s' % (cache_status, line))
                    continue
                cache_status = httpStatuses[cache_status]

                if uri == 'NONE://' or (http_method == 'CONNECT' and uri == ':0'):
                    uri_host = uri_path = uri_query = ''

                else:
                    m = self.urlRe.match(uri)
                    if not m:
                        safePrint(u'URL parsing failed: "%s"\n%s' % (uri, line))
                        continue
                    if m.group(1).lower() == u'https' and u'https=' not in x_analytics:
                        x_analytics += u';https=1'
                    uri_host = m.group(2)
                    if uri_host.endswith(':80'):
                        uri_host = uri_host[:-3]
                    if uri_host.endswith('.'):
                        uri_host = uri_host[:-1]
                    uri_path = m.group(3)
                    uri_query = m.group(4)

                result = '\t'.join(
                    [hostname, sequence, dt, time_firstbyte, ip, cache_status, http_status, response_size, http_method,
                     uri_host, uri_path, uri_query, content_type, referer, x_forwarded_for, user_agent, accept_language,
                     x_analytics, webrequest_source, year, month, day, hour])
                out.write(result + '\n')

        if os.path.exists(statFile):
            os.remove(statFile)
        os.rename(tmpFile, statFile)


    def run(self):
        self.processLogFiles()

    def manualRun(self):
        self.processLogFiles()


if __name__ == '__main__':
    # LogConverter(filePattern=(sys.argv[1] if len(sys.argv) > 1 else False)).manualRun()
    LogConverter(filePattern=(sys.argv[1] if len(sys.argv) > 1 else False)).safeRun()
