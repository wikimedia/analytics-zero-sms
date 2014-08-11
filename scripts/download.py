# coding=utf-8
import pipes
import string
import subprocess
import locale
from datetime import datetime, timedelta
import re
import os
import json
import traceback

from boto.s3.connection import S3Connection
import io
import itertools
import generate


def generatePassword(size=10, chars=string.ascii_letters + string.digits):
    """ Adapted from
    http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
    """
    import random
    return ''.join(random.choice(chars) for _ in range(size))


class Processor(object):
    dateFormat = '%Y-%m-%d'

    def __init__(self, dataDir='data', workDir='state', graphDir='graphs', settingsFile='settings.json'):
        self.dataDir = dataDir
        self.workDir = workDir
        self.graphDir = graphDir
        if not os.path.exists(dataDir): os.mkdir(dataDir)
        if not os.path.exists(workDir): os.mkdir(workDir)
        if not os.path.exists(graphDir): os.mkdir(graphDir)

        self.settingsFilePath = os.path.join(self.workDir, settingsFile)
        self.combinedFilePath = os.path.join(self.workDir, 'combined.tsv')
        self.statsFilePath = os.path.join(self.workDir, 'combined.json')

        data = self.loadState()
        self.awsBucket = data['awsBucket'] if 'awsBucket' in data else generatePassword()
        self.awsKeyId = data['awsKeyId'] if 'awsKeyId' in data else generatePassword()
        self.awsSecret = data['awsSecret'] if 'awsSecret' in data else generatePassword()
        self.awsUser = data['awsUser'] if 'awsUser' in data else generatePassword()
        self.downloadOverlapDays = data['downloadOverlapDays'] if 'downloadOverlapDays' in data else False
        self.enableDownload = data['enableDownload'] if 'enableDownload' in data else True
        self.enableDownloadOld = data['enableDownloadOld'] if 'enableDownloadOld' in data else True
        self.lastDownloadTs = self.parseDate(data['lastDownloadTs']) if 'lastDownloadTs' in data else False
        self.lastErrorMsg = data['lastErrorMsg'] if 'lastErrorMsg' in data else False
        self.lastErrorTs = self.parseDate(data['lastErrorTs']) if 'lastErrorTs' in data else False
        self.lastGoodRunTs = self.parseDate(data['lastGoodRunTs']) if 'lastGoodRunTs' in data else False
        self.lastProcessedTs = self.parseDate(data['lastProcessedTs']) if 'lastProcessedTs' in data else False
        self.partnerDirMap = data['partnerDirMap'] if 'partnerDirMap' in data else {}
        self.partnerMap = data['partnerMap'] if 'partnerMap' in data else {}
        self.processOverlapDays = data['processOverlapDays'] if 'processOverlapDays' in data else 1
        self.salt = data['salt'] if 'salt' in data else generatePassword()
        self.smtpFrom = data['smtpFrom'] if 'smtpFrom' in data else False
        self.smtpHost = data['smtpHost'] if 'smtpHost' in data else False
        self.smtpTo = data['smtpTo'] if 'smtpTo' in data else False
        self.sortCmd = data['sortCmd'] if 'sortCmd' in data else 'sort'

        if self.downloadOverlapDays and self.lastDownloadTs:
            self.downloadIfAfter = self.lastDownloadTs - timedelta(days=self.downloadOverlapDays)
        else:
            self.downloadIfAfter = False

        if self.lastProcessedTs:
            self.processIfAfter = self.lastProcessedTs - timedelta(days=self.processOverlapDays)
        else:
            self.processIfAfter = False

        # wikipedia_application_3.log.2014-06-11
        self.fileRe = re.compile(ur'^wikipedia_application_\d+\.log\.(?P<date>\d{4}-\d{2}-\d{2})$', re.IGNORECASE)

    def loadState(self):
        if os.path.isfile(self.settingsFilePath):
            with io.open(self.settingsFilePath, 'rb') as f:
                return json.load(f)
        return {}

    def saveState(self):
        data = self.loadState()
        data['awsBucket'] = self.awsBucket
        data['awsKeyId'] = self.awsKeyId
        data['awsSecret'] = self.awsSecret
        data['awsUser'] = self.awsUser
        data['downloadOverlapDays'] = int(self.downloadOverlapDays) if self.downloadOverlapDays else False
        data['enableDownload'] = self.enableDownload
        data['enableDownloadOld'] = self.enableDownloadOld
        data['lastDownloadTs'] = self.formatDate(self.lastDownloadTs)
        data['lastErrorMsg'] = self.lastErrorMsg
        data['lastErrorTs'] = self.formatDate(self.lastErrorTs)
        data['lastGoodRunTs'] = self.formatDate(self.lastGoodRunTs)
        data['lastProcessedTs'] = self.formatDate(self.lastProcessedTs)
        data['partnerDirMap'] = self.partnerDirMap
        data['partnerMap'] = self.partnerMap
        data['processOverlapDays'] = int(self.processOverlapDays) if self.processOverlapDays else False
        data['salt'] = self.salt
        data['smtpFrom'] = self.smtpFrom
        data['smtpHost'] = self.smtpHost
        data['smtpTo'] = self.smtpTo
        data['sortCmd'] = self.sortCmd

        stateBk = self.settingsFilePath + '.bak'
        with open(stateBk, 'wb') as f:
            json.dump(data, f, indent=True, sort_keys=True)
        if os.path.exists(self.settingsFilePath):
            os.remove(self.settingsFilePath)
        os.rename(stateBk, self.settingsFilePath)

    def parseDate(self, value):
        return datetime.strptime(str(value), self.dateFormat) if isinstance(value, basestring) else value

    def formatDate(self, value):
        return value.strftime(self.dateFormat) if isinstance(value, datetime) else value

    def getFileDate(self, filename):
        m = self.fileRe.match(filename)
        return self.parseDate(m.group('date')) if m else False

    def download(self):
        print('\nDownloading files')

        cn = S3Connection(self.awsKeyId, self.awsSecret)
        prefix = 'prd-vumi-wikipedia.aws.prk-host.net/'
        bucket = cn.get_bucket(self.awsBucket)
        files = bucket.list(prefix)

        for key in files:
            filename = key.key[len(prefix):]
            filePath = os.path.join(self.dataDir, filename)
            fileDate = self.getFileDate(filename)
            fileExists = os.path.exists(filePath)

            if not self.enableDownloadOld and not fileDate:
                print('Skipping legacy-named file %s' % filename)
                continue
            elif key.size == 0:
                print('Skipping empty file %s' % filename)
                continue
            elif not fileExists:
                reason = u"it doesn't exist"
            elif key.size != os.stat(filePath).st_size:
                reason = u'local size %s <> remote %s' % (
                    locale.format(u"%d", os.stat(filePath).st_size, grouping=True),
                    locale.format(u"%d", key.size, grouping=True))
            elif fileDate and self.downloadIfAfter and fileDate > self.downloadIfAfter:
                reason = u'date is too close to last file date %s' % self.downloadIfAfter
            else:
                continue

            print('Downloading %s because %s' % (filename, reason))
            if fileExists:
                if os.stat(filePath).st_size == 0:
                    print('Removing empty file %s' % filePath)
                    os.remove(filePath)
                else:
                    bakCount = 0
                    bakFile = filePath + '.bak'
                    while os.path.exists(bakFile):
                        bakCount += 1
                        bakFile = filePath + '.bak' + str(bakCount)
                    print('Renaming %s => %s' % (filePath, bakFile))
                    os.rename(filePath, bakFile)

            key.get_contents_to_filename(filePath)
            if fileDate and (not self.lastDownloadTs or self.lastDownloadTs < fileDate):
                self.lastDownloadTs = fileDate

    def combineDataFiles(self, sourceFiles):

        print('Combining files into %s' % self.combinedFilePath)
        print('Processing %s' % (('files on or after %s' % self.processIfAfter) if self.processIfAfter else 'all files'))

        appendingDataFile = self.combinedFilePath + '.tmp'
        manualLogRe = re.compile(ur'^wikipedia_application_\d+\.log\.\d+\.gz\:')

        totalCount = 0
        with io.open(appendingDataFile, 'w', encoding='utf8') as dst:
            for srcFile in sourceFiles:

                fileDate = self.getFileDate(srcFile)
                if self.processIfAfter:
                    if not fileDate:
                        continue  # old style filename, and the processIfAfter is set
                    elif fileDate <= self.processIfAfter:
                        continue  # we have already processed this file

                srcFilePath = os.path.join(self.dataDir, srcFile)
                if not os.path.isfile(srcFilePath):
                    print('File %s was not found, skipping' % srcFilePath)
                    continue
                last = False
                count = 0
                for line in io.open(srcFilePath, 'r', encoding='utf8'):
                    count += 1
                    totalCount += 1
                    if count == 1 or totalCount % 30000 == 0:
                        print('File %s, line %d, total lines %d' % (srcFile, count-1, totalCount-1))

                    l = line.strip(u'\n\r')
                    l = manualLogRe.sub( '', l, 1 )
                    if u' WIKI\t' in l:
                        self.writeLine(dst, last)
                        last = l
                    elif len(l) > 2 and l[0] == u'2' and l[1] == u'0':
                        self.writeLine(dst, last)
                        last = False
                    elif isinstance(last, basestring):
                        last = last + '\t' + l

                self.writeLine(dst, last)
                if fileDate and (not self.lastProcessedTs or self.lastProcessedTs < fileDate):
                    self.lastProcessedTs = fileDate

        if totalCount > 0:
            # Sort files into one
            sortedOutputFile = self.combinedFilePath + '.out'
            if os.path.exists(sortedOutputFile): os.remove(sortedOutputFile)

            args = [self.sortCmd, '-u', '-o', sortedOutputFile, appendingDataFile]
            originalExists = os.path.exists(self.combinedFilePath)
            if originalExists:
                args.append(self.combinedFilePath)
            cmd = ' '.join([pipes.quote(v) for v in args])
            print('\nSorting: %s' % cmd)
            try:
                tmp2 = sortedOutputFile + '2'
                if os.path.exists(tmp2):
                    os.remove(tmp2)

                subprocess.check_output(args, stderr=subprocess.STDOUT)

                # Extra safety - keep old file until we rename temp to its name
                if originalExists:
                    os.rename(self.combinedFilePath, tmp2)
                os.rename(sortedOutputFile, self.combinedFilePath)
                if originalExists:
                    os.remove(tmp2)

            except subprocess.CalledProcessError, ex:
                raise Exception(u'Error %s running %s\nOutput:\n%s' % (ex.returncode, cmd, ex.output))

        os.remove(appendingDataFile)

    def writeLine(self, dst, line):
        if not line:
            return
        line = line.replace(u'\0', u'\\0')
        parts = line.split('\t')
        if parts[1][0] == u'+':
            return
        parts = [p[2:-1]
                 if (p.startswith(u"u'") and p.endswith(u"'")) or (p.startswith(u'u"') and p.endswith(u'"'))
                 else p for p in parts]
        tmp = parts[0]
        parts[0] = parts[1]
        parts[1] = tmp\
            .replace(u' [VumiRedis,client]', u'') \
            .replace(u' [HTTP11ClientProtocol,client]', u'') \
            .replace(u' WIKI', u'') \
            .replace(u'+0000', u'')

        if len(parts) > 5 and parts[5].startswith(u'content='):
            parts[5] = u'content=' + str(len(parts[5]) - 10)

        if len(parts) > 6:
            parts[6] = parts[6].replace('\0', '\\0')

        dst.write('\t'.join(parts) + '\n')

    def error(self, error):
        self.lastErrorTs = datetime.now()
        self.lastErrorMsg = error

        print(error)

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

        s = smtplib.SMTP(self.smtpHost)
        s.sendmail(self.smtpFrom, self.smtpTo, msg.as_string().encode('ascii'))
        s.quit()

    def generateGraphData(self, skipParsing=False):
        stats = generate.Stats(self.combinedFilePath, self.graphDir, self.statsFilePath, self.partnerMap, self.partnerDirMap, self.salt)

        if not skipParsing:
            print('\nParsing data')
            stats.process()
            stats.pickle()
        else:
            print('Loading parsed data')
            stats.unpickle()

        print('Generating data files to %s' % self.graphDir)
        # stats.dumpStats()
        stats.createGraphs()

    def run(self):
        # noinspection PyBroadException
        try:
            if self.enableDownload:
                self.download()
            files = os.listdir(self.dataDir)
            files = itertools.chain([os.path.join('pc', f) for f in os.listdir(os.path.join(self.dataDir, 'pc'))], files)
            self.combineDataFiles(files)

            self.generateGraphData()
            self.lastGoodRunTs = datetime.now()
        except:
            self.error(traceback.format_exc())
        self.saveState()


if __name__ == "__main__":
    prc = Processor()
    prc.run()
