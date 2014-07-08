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

import private


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
        self.tempFilePath = os.path.join(self.workDir, 'temp.tsv')
        if os.path.exists(self.tempFilePath): os.remove(self.tempFilePath)
        self.statsFilePath = os.path.join(self.workDir, 'combined.json')
        if os.path.exists(self.statsFilePath): os.remove(self.statsFilePath)

        data = self.loadState()

        self.enableDownload = data['enableDownload'] if 'enableDownload' in data else True
        self.enableDownloadOld = data['enableDownloadOld'] if 'enableDownloadOld' in data else True
        self.lastDownloadTs = self.parseDate(data['lastDownloadTs']) if 'lastDownloadTs' in data else False
        self.downloadOverlapDays = data['downloadOverlapDays'] if 'downloadOverlapDays' in data else False
        self.lastProcessedTs = self.parseDate(data['lastProcessedTs']) if 'lastProcessedTs' in data else False
        self.processOverlapDays = data['processOverlapDays'] if 'processOverlapDays' in data else 1
        self.smtpHost = data['smtpHost'] if 'smtpHost' in data else False
        self.smtpFrom = data['smtpFrom'] if 'smtpFrom' in data else False
        self.smtpTo = data['smtpTo'] if 'smtpTo' in data else False
        self.lastErrorTs = self.parseDate(data['lastErrorTs']) if 'lastErrorTs' in data else False
        self.lastErrorMsg = data['lastErrorMsg'] if 'lastErrorMsg' in data else False
        self.sortCmd = data['sortCmd'] if 'sortCmd' in data else 'sort'
        self.lastGoodRunTs = self.parseDate(data['lastGoodRunTs']) if 'lastGoodRunTs' in data else False
        self.partnerMap = data['partnerMap'] if 'partnerMap' in data else {}
        self.partnerDirMap = data['partnerDirMap'] if 'partnerDirMap' in data else {}
        self.salt = data['salt'] if 'salt' in data else generatePassword()

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
        data['enableDownload'] = self.enableDownload
        data['enableDownloadOld'] = self.enableDownloadOld
        data['lastDownloadTs'] = self.formatDate(self.lastDownloadTs)
        data['downloadOverlapDays'] = int(self.downloadOverlapDays) if self.downloadOverlapDays else False
        data['lastProcessedTs'] = self.formatDate(self.lastProcessedTs)
        data['processOverlapDays'] = int(self.processOverlapDays) if self.processOverlapDays else False
        data['smtpHost'] = self.smtpHost
        data['smtpFrom'] = self.smtpFrom
        data['smtpTo'] = self.smtpTo
        data['lastErrorTs'] = self.formatDate(self.lastErrorTs)
        data['lastErrorMsg'] = self.lastErrorMsg
        data['sortCmd'] = self.sortCmd
        data['lastGoodRunTs'] = self.formatDate(self.lastGoodRunTs)
        data['partnerMap'] = self.partnerMap
        data['partnerDirMap'] = self.partnerDirMap
        data['salt'] = self.salt

        stateBk = self.settingsFilePath + '.bak'
        with open(stateBk, 'wb') as f:
            json.dump(data, f, indent=True, sort_keys=True)
        if os.path.exists(self.statsFilePath):
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
        cn = S3Connection(private.aws_access_key_id, private.aws_secret_access_key)
        prefix = 'prd-vumi-wikipedia.aws.prk-host.net/'
        bucket = cn.get_bucket(private.bucket_name)
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

        print 'Combining files into %s' % self.combinedFilePath
        print 'Processing %s' % (('files on or after %s' % self.processIfAfter) if self.processIfAfter else 'all files')
        with io.open(self.combinedFilePath, 'a', encoding='utf8') as dst:
            totalCount = 0
            for srcFile in sourceFiles:

                fileDate = self.getFileDate(srcFile)
                if self.processIfAfter:
                    if not fileDate:
                        continue # old style filename, and the processIfAfter is set
                    elif fileDate <= self.processIfAfter:
                        continue # we have already processed this file

                srcFilePath = os.path.join(self.dataDir, srcFile)
                if not os.path.isfile(srcFilePath):
                    print 'File %s was not found, skipping' % srcFilePath
                    continue
                last = False
                count = 0
                for line in io.open(srcFilePath, 'r', encoding='utf8'):
                    count += 1
                    totalCount += 1
                    if count == 1 or totalCount % 30000 == 0:
                        print('File %s, line %d, total lines %d' % (srcFile, count-1, totalCount-1))

                    l = line.strip('\n\r')
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
            .replace(u' [VumiRedis,client] WIKI', u'') \
            .replace(u' [HTTP11ClientProtocol,client] WIKI', u'') \
            .replace(u'+0000', u'')

        if len(parts) > 5 and parts[5].startswith(u'content='):
            parts[5] = u'content=' + str(len(parts[5]) - 10)

        if len(parts) > 6:
            parts[6] = parts[6].replace('\0', '\\0')

        dst.write('\t'.join(parts) + '\n')

    def sort(self):

        args = [self.sortCmd, '-u', '-o', self.tempFilePath, self.combinedFilePath]
        cmd = ' '.join([pipes.quote(v) for v in args])
        print('\nSorting: %s' % cmd)
        try:
            tmp2 = self.tempFilePath + '2'
            if os.path.exists(tmp2):
                os.remove(tmp2)

            subprocess.check_output(args, stderr=subprocess.STDOUT)

            # Extra safety - keep old file until we rename temp to its name
            os.rename(self.combinedFilePath, tmp2)
            os.rename(self.tempFilePath, self.combinedFilePath)
            os.remove(tmp2)

        except subprocess.CalledProcessError, ex:
            raise Exception(u'Error %s running %s\nOutput:\n%s' % (ex.returncode, cmd, ex.output))

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
        # if ((parts[2] == 'airtel_ke_ussd_transport' and parts[3] == 'ussd' and parts[4] == 'airtel') or
        # (parts[2] == 'airtel_ke_sms_transport' and parts[3] == 'sms' and parts[4] == '')):
        #     parts[2:5] = ['airtel']
        # elif ((parts[2] == 'vumi_starcode_transport' and parts[3] == 'ussd' and parts[4] == '') or
        #       (parts[2] == 'smpp_transport' and parts[3] == 'sms' and parts[4] == '')):
        #     parts[2:5] = ['vumi']
        # elif parts[2] == 'zambia_cellulant_ussd_transport' and parts[3] == 'ussd' and parts[4] == '':
        #     parts[2:5] = ['zambia-cellulant']
        # elif parts[2] == 'ambient_go_smpp_transport' and parts[3] == 'sms' and parts[4] == '':
        #     parts[2:5] = ['ambient_go']
        # elif ((parts[2] == 'truteq_8864_transport' or parts[2] == 'truteq_32323_transport') and parts[3] == 'ussd' and
        #       parts[4] == ''):
        #     parts[2:5] = ['truteq']
        # elif parts[2] == 'equity_kenya_ussd_smpp_transport' and parts[3] == 'ussd' and parts[4] == '':
        #     parts[2:5] = ['equity_ke']
        # else:
        #     raise BaseException(line)

        stats = generate.Stats(self.combinedFilePath, self.graphDir, self.statsFilePath, self.partnerMap, self.partnerDirMap, self.salt)

        if not skipParsing:
            print('\nParsing data')
            stats.process()
            stats.pickle()
        else:
            print('Loading parsed data')
            stats.unpickle()

        print('Generating data files to to %s' % self.graphDir)
        # stats.dumpStats()
        stats.createGraphs()

    def run(self):
        # noinspection PyBroadException
        try:
            # if self.enableDownload:
            #     self.download()
            # files = os.listdir(self.dataDir)
            # # files = itertools.chain([os.path.join('pc', f) for f in os.listdir(os.path.join(self.dataDir, 'pc'))], files)
            # self.combineDataFiles(files)
            # self.sort()

            self.generateGraphData(True)
            self.lastGoodRunTs = datetime.now()
        except:
            self.error(traceback.format_exc())
        self.saveState()


if __name__ == "__main__":
    prc = Processor()
    prc.run()