# coding=utf-8
import gzip
from datetime import datetime
import os
import json
import traceback
import re
import io
import collections
import sys

try:
    from unidecode import unidecode
except ImportError:
    unidecode = lambda txt: txt.encode('ascii', 'replace')


def safePrint(text):
    print(unidecode(unicode(text)))


def saveJson(filename, data):
    with open(filename, 'wb') as f:
        json.dump(data, f, indent=True, sort_keys=True)


def loadJson(filename):
    with io.open(filename, 'rb') as f:
        return json.load(f)


def saveData(filename, data):
    with io.open(filename, 'w', encoding='utf8', errors='ignore') as out:
        out.writelines(['\t'.join(i) + '\n' for i in data])


def loadData(filename):
    with io.open(filename, 'r', encoding='utf8', errors='ignore') as inp:
        for line in inp:
            yield line.strip('\r\n').split('\t')


def addStat(stats, date, dataType, xcs, via, ipset, https, lang, subdomain, site):
    key = (date, dataType, xcs, via, ipset, 'https' if https else 'http', lang, subdomain, site)
    if key in stats:
        stats[key] += 1
    else:
        datetime.strptime(date, '%Y-%m-%d')  # Validate date - slow operation, do it only once per key
        stats[key] = 1


class LogProcessor(object):
    def __init__(self, settingsFile='settings/weblogs.json', logDatePattern=False):

        self.settingsFile = self.normalizePath(settingsFile, False)

        data = self.loadState()
        self.lastErrorMsg = data['lastErrorMsg'] if 'lastErrorMsg' in data else False
        self.lastErrorTs = data['lastErrorTs'] if 'lastErrorTs' in data else False
        self.lastGoodRunTs = data['lastGoodRunTs'] if 'lastGoodRunTs' in data else False
        self.lastProcessedTs = data['lastProcessedTs'] if 'lastProcessedTs' in data else False
        self.smtpFrom = data['smtpFrom'] if 'smtpFrom' in data else False
        self.smtpHost = data['smtpHost'] if 'smtpHost' in data else False
        self.smtpTo = data['smtpTo'] if 'smtpTo' in data else False
        self.username = data['apiUsername'] if 'apiUsername' in data else ''
        self.password = data['apiPassword'] if 'apiPassword' in data else ''
        self.rawPathLogs = data['pathLogs'] if 'pathLogs' in data else ''
        self.rawPathStats = data['pathStats'] if 'pathStats' in data else ''
        self.rawPathGraphs = data['pathGraphs'] if 'pathGraphs' in data else ''
        self.saveState()

        if not self.rawPathLogs or not self.rawPathStats or not self.rawPathGraphs:
            raise ValueError('One of the paths is not set, check %s' % settingsFile)

        self.pathLogs = self.normalizePath(self.rawPathLogs)
        self.pathStats = self.normalizePath(self.rawPathStats)
        self.pathGraphs = self.normalizePath(self.rawPathGraphs)

        # zero.tsv.log-20140808.gz
        if not logDatePattern:
            logDatePattern = r'\d+'
        logReStr = r'zero\.tsv\.log-(' + logDatePattern + ')\.gz'
        self.logFileRe = re.compile(r'^' + logReStr + r'$', re.IGNORECASE)
        self.statFileRe = re.compile(r'^(' + logReStr + r')__\d+\.tsv$', re.IGNORECASE)
        self.urlRe = re.compile(r'^https?://([^/]+)', re.IGNORECASE)
        self.duplUrlRe = re.compile(r'^(https?://.+)\1', re.IGNORECASE)
        self.zcmdRe = re.compile(r'zcmd=([^&]+)', re.IGNORECASE)

    def saveState(self):
        fmt = lambda v: v.strftime('%Y-%m-%d %H:%M:%S') if isinstance(v, datetime) else v

        data = self.loadState()
        data['lastErrorMsg'] = self.lastErrorMsg
        data['lastErrorTs'] = fmt(self.lastErrorTs)
        data['lastGoodRunTs'] = fmt(self.lastGoodRunTs)
        data['lastProcessedTs'] = fmt(self.lastProcessedTs)
        data['smtpFrom'] = self.smtpFrom
        data['smtpHost'] = self.smtpHost
        data['smtpTo'] = self.smtpTo
        data['apiUsername'] = self.username
        data['apiPassword'] = self.password
        data['pathLogs'] = self.rawPathLogs
        data['pathStats'] = self.rawPathStats
        data['pathGraphs'] = self.rawPathGraphs

        stateBk = self.settingsFile + '.bak'
        saveJson(stateBk, data)
        if os.path.exists(self.settingsFile):
            os.remove(self.settingsFile)
        os.rename(stateBk, self.settingsFile)

    def loadState(self):
        if os.path.isfile(self.settingsFile):
            return loadJson(self.settingsFile)
        return {}

    def downloadConfigs(self):
        import api

        site = api.wikimedia('zero', 'wikimedia', 'https')
        site.login(self.username, self.password)
        # https://zero.wikimedia.org/w/api.php?action=zeroportal&type=analyticsconfig&format=jsonfm
        configs = site('zeroportal', type='analyticsconfig')['zeroportal']
        for cfs in configs.values():
            for c in cfs:
                c['from'] = datetime.strptime(c['from'], '%Y-%m-%dT%H:%M:%SZ')
                if c['before'] is None:
                    c['before'] = datetime.max
                else:
                    c['before'] = datetime.strptime(c['before'], '%Y-%m-%dT%H:%M:%SZ')
                c['languages'] = True if True == c['languages'] else set(c['languages'])
                c['sites'] = True if True == c['sites'] else set(c['sites'])
                c['via'] = set(c['via'])
                c['ipsets'] = set(c['ipsets'])
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
            statFile = os.path.join(self.pathStats, f + '__' + unicode(logSize) + '.tsv')
            statFiles[f] = statFile
            if not os.path.exists(statFile):
                fileDt = m.group(1)
                fileDt = '-'.join([fileDt[0:4], fileDt[4:6], fileDt[6:8]])
                self.processLogFile(logFile, statFile, fileDt)

        # Clean up older stat files (if gz file size has changed)
        removeFiles = []
        for f in os.listdir(self.pathStats):
            m = self.statFileRe.match(f)
            if not m:
                continue
            logFile = m.group(1)
            statFile = os.path.join(self.pathStats, f)
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
        validSubDomains = {'m', 'zero', 'mobile', 'wap'}
        validHttpCode = {'200', '304'}

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
            analytics = dict([x.split('=', 2) for x in set(analytics.split(';'))])
            xcs = analytics['zero'].rstrip('|') if 'zero' in analytics else None
            tmp = l[5].split('/', 2)
            if len(tmp) < 2:
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

        saveData(statFile, [list(k) + [unicode(v)] for k,v in stats.items()])

    def combineStats(self, tempFile=''):
        safePrint('Combine stat files')
        configs = self.downloadConfigs()
        stats = collections.defaultdict(int)
        for f in os.listdir(self.pathStats):
            if not self.statFileRe.match(f):
                continue
            for k, v in loadJson(os.path.join(self.pathStats, f)).items():
                # "2014-08-07|250-99|DIRECT|default|https|ru|m.wikipedia"
                kp = k.split('|')
                if len(kp) != 7:
                    if len(kp) == 8 and kp[2] == '':
                        safePrint('Fixing key %s in file %s' % (k, f))
                        del kp[2]
                    else:
                        raise ValueError('Unrecognized key %s in file %s' % (k, f))
                (dt, xcs, via, ipset, https, lang, site) = kp
                dt = datetime.strptime(dt, '%Y-%m-%d')

                isZero = False
                if xcs in configs:
                    for conf in configs[xcs]:
                        langs = conf['languages']
                        sites = conf['sites']
                        if conf['from'] <= dt < conf['before'] and \
                                (not https or conf['https']) and \
                                (True == langs or lang in langs) and \
                                (True == sites or site in sites) and \
                                (via in conf['via']) and \
                                (ipset in conf['ipsets']):
                            isZero = True
                            break
                status = 'INCL' if isZero else 'EXCL'
                stats[k + '|' + status] += v

        # convert {"a|b|c":count,...}  into [[a,b,c,count],...]
        stats = [k.split('|') + [unicode(v)] for k, v in stats.items()]
        if tempFile:
            with open(tempFile, "w") as out:
                out.writelines(['\t'.join(i) + '\n' for i in stats])
        return stats

    def generateGraphData(self, stats):
        safePrint('Generating data files to %s' % self.pathGraphs)

    def error(self, error):
        self.lastErrorTs = datetime.now()
        self.lastErrorMsg = error

        safePrint(error)

        if not self.smtpHost or not self.smtpFrom or not self.smtpTo:
            return

        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'SMS report error'
        msg.attach(MIMEText(error, 'plain', 'utf-8'))

        # m = MIMEText(error, 'plain', 'utf-8')
        # m['From'] = self.smtpFrom
        # m['To'] = self.smtpTo
        # m['Subject'] = msg['Subject']

        smtp = smtplib.SMTP(self.smtpHost)
        smtp.sendmail(self.smtpFrom, self.smtpTo, msg.as_string().encode('ascii'))
        smtp.quit()

    def run(self):
        # noinspection PyBroadException
        try:
            self.processLogFiles()
            stats = self.combineStats()
            self.generateGraphData(stats)
            self.lastGoodRunTs = datetime.now()
        except:
            self.error(traceback.format_exc())
        self.saveState()

    def normalizePath(self, path, relToSettings=True):
        if not os.path.isabs(path) and relToSettings:
            path = os.path.join(os.path.dirname(self.settingsFile), path)
        path = os.path.abspath(os.path.normpath(path))
        dirPath = path if relToSettings else os.path.dirname(path)
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        return path


    def reformatArch(self):
        for f in os.listdir(self.pathStats):
            if not self.statFileRe.match(f): continue
            pth = os.path.join(self.pathStats, f)
            isFirst = True
            values = []
            for parts in loadData(pth):
                if len(parts) != 9:
                    if isFirst: break
                    raise ValueError('Bad file line %s' % f)
                isFirst = False
                if parts[1] == 'DATA':
                    pp = parts[7].split('.', 2)
                    parts[7] = pp[1] if len(pp) > 1 else pp[0]
                    parts.insert(7, pp[0] if len(pp) > 1 else '')
                else:
                    parts.insert(6, '')
                values.append(parts)

            saveData(pth + '.out', values)


if __name__ == "__main__":

    prc = LogProcessor(logDatePattern=(sys.argv[1] if len(sys.argv) > 1 else False))

    # prc.reformatArch()
    # prc.run()

    prc.processLogFiles()
    # s = prc.combineStats(os.path.join(prc.pathStats, 'combined-tmp.tsv'))

    # file = r'c:\Users\user\mw\shared\zero-sms\data\weblogs\zero.tsv.log-20140808.gz'
    # prc.processLogFile(file, file + '.json')

    # file = r'c:\Users\user\mw\shared\zero-sms\data\weblogs\zero.tsv.log-20140808.gz'
    # prc.processLogFile(file, file + '.json')

    # prc.downloadConfigs()

# prc.run()
