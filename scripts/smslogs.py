# coding=utf-8
import pipes
import string
import subprocess
import locale
from datetime import timedelta
import re
import traceback
import itertools

from boto.s3.connection import S3Connection

import smsgraphs
from logprocessor import *


def generatePassword(size=10, chars=string.ascii_letters + string.digits):
    """ Adapted from
    http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
    """
    import random

    return ''.join(random.choice(chars) for _ in range(size))


class SmsLogProcessor(LogProcessor):
    dateFormat = '%Y-%m-%d'

    def __init__(self, settingsFile='settings/smslogs.json'):
        super(SmsLogProcessor, self).__init__(settingsFile, 'web')

        self.combinedFilePath = os.path.join(self.pathCache, 'combined.tsv')
        self.statsFilePath = os.path.join(self.pathCache, 'combined.json')

        if self.settings.downloadOverlapDays and self.settings.lastDownloadTs:
            self.downloadIfAfter = self.settings.lastDownloadTs - timedelta(days=self.settings.downloadOverlapDays)
        else:
            self.downloadIfAfter = False

        if self.settings.lastProcessedTs:
            self.processIfAfter = self.settings.lastProcessedTs - timedelta(days=self.settings.processOverlapDays)
        else:
            self.processIfAfter = False

        # wikipedia_application_3.log.2014-06-11
        self.fileRe = re.compile(r'^wikipedia_application_\d+\.log\.(?P<date>\d{4}-\d{2}-\d{2})$', re.IGNORECASE)

    def defaultSettings(self, suffix):
        s = super(SmsLogProcessor, self).defaultSettings(suffix)
        s.awsBucket = generatePassword()
        s.awsKeyId = generatePassword()
        s.awsSecret = generatePassword()
        s.awsPrefix = ''
        s.awsUser = generatePassword()
        s.downloadOverlapDays = 0
        s.enableDownload = True
        s.enableDownloadOld = True
        s.lastDownloadTs = False
        s.lastProcessedTs = False
        s.partnerDirMap = {}
        s.partnerMap = {}
        s.processOverlapDays = 1
        s.salt = generatePassword()
        s.sortCmd = 'sort'
        return s

    def onSavingSettings(self):
        super(SmsLogProcessor, self).onSavingSettings()
        s = self.settings
        s.lastDownloadTs = self.formatDate(s.lastDownloadTs, self.dateFormat)
        s.lastProcessedTs = self.formatDate(s.lastProcessedTs, self.dateFormat)

    def onSettingsLoaded(self):
        super(SmsLogProcessor, self).onSettingsLoaded()
        s = self.settings
        s.lastDownloadTs = self.parseDate(s.lastDownloadTs, self.dateFormat)
        s.lastProcessedTs = self.parseDate(s.lastProcessedTs, self.dateFormat)

    def getFileDate(self, filename):
        m = self.fileRe.match(filename)
        return self.parseDate(m.group('date'), self.dateFormat) if m else False

    def download(self):
        safePrint(u'\nDownloading files')

        cn = S3Connection(self.settings.awsKeyId, self.settings.awsSecret)

        bucket = cn.get_bucket(self.settings.awsBucket)
        files = bucket.list(self.settings.awsPrefix)

        for key in files:
            filename = key.key[len(self.settings.awsPrefix):]
            filePath = os.path.join(self.pathLogs, filename)
            fileDate = self.getFileDate(filename)
            fileExists = os.path.exists(filePath)

            if key.size == 0:
                safePrint(u'Skipping empty file %s' % filename)
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

            if not self.settings.enableDownloadOld and not fileDate:
                safePrint(u'Skipping legacy-named file %s even though %s' % (filename, reason))
                continue

            safePrint(u'Downloading %s because %s' % (filename, reason))
            if fileExists:
                if os.stat(filePath).st_size == 0:
                    safePrint(u'Removing empty file %s' % filePath)
                    os.remove(filePath)
                else:
                    bakCount = 0
                    bakFile = filePath + '.bak'
                    while os.path.exists(bakFile):
                        bakCount += 1
                        bakFile = filePath + '.bak' + str(bakCount)
                    safePrint(u'Renaming %s => %s' % (filePath, bakFile))
                    os.rename(filePath, bakFile)

            key.get_contents_to_filename(filePath)
            if fileDate and (not self.settings.lastDownloadTs or self.settings.lastDownloadTs < fileDate):
                self.settings.lastDownloadTs = fileDate

    def combineDataFiles(self, sourceFiles):

        safePrint(u'Combining files into %s' % self.combinedFilePath)
        if self.processIfAfter:
            safePrint(u'Processing files on or after %s' % self.processIfAfter)
        else:
            safePrint(u'Processing all files')

        appendingDataFile = self.combinedFilePath + '.tmp'
        manualLogRe = re.compile(r'^wikipedia_application_\d+\.log\.\d+\.gz:')

        totalCount = 0
        with io.open(appendingDataFile, 'w', encoding='utf8') as dst:
            for srcFile in sourceFiles:

                fileDate = self.getFileDate(srcFile)
                if self.processIfAfter:
                    if not fileDate:
                        continue  # old style filename, and the processIfAfter is set
                    elif fileDate <= self.processIfAfter:
                        continue  # we have already processed this file

                srcFilePath = os.path.join(self.pathLogs, srcFile)
                if not os.path.isfile(srcFilePath):
                    safePrint(u'File %s was not found, skipping' % srcFilePath)
                    continue
                last = False
                count = 0
                for line in io.open(srcFilePath, 'r', encoding='utf8'):
                    count += 1
                    totalCount += 1
                    if count == 1 or totalCount % 30000 == 0:
                        safePrint(u'File %s, line %d, total lines %d' % (srcFile, count - 1, totalCount - 1))

                    l = line.strip(u'\n\r')
                    l = manualLogRe.sub('', l, 1)
                    if u' WIKI\t' in l:
                        self.writeLine(dst, last)
                        last = l
                    elif len(l) > 2 and l[0] == u'2' and l[1] == u'0':
                        self.writeLine(dst, last)
                        last = False
                    elif isinstance(last, basestring):
                        last = last + '\t' + l

                self.writeLine(dst, last)
                if fileDate and (not self.settings.lastProcessedTs or self.settings.lastProcessedTs < fileDate):
                    self.settings.lastProcessedTs = fileDate

        if totalCount > 0:
            # Sort files into one
            sortedOutputFile = self.combinedFilePath + '.out'
            if os.path.exists(sortedOutputFile):
                os.remove(sortedOutputFile)

            args = [self.settings.sortCmd, '-u', '-o', sortedOutputFile, appendingDataFile]
            originalExists = os.path.exists(self.combinedFilePath)
            if originalExists:
                args.append(self.combinedFilePath)
            cmd = ' '.join([pipes.quote(v) for v in args])
            safePrint(u'\nSorting: %s' % cmd)
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
        parts[1] = tmp \
            .replace(u' [VumiRedis,client]', u'') \
            .replace(u' [HTTP11ClientProtocol,client]', u'') \
            .replace(u' WIKI', u'') \
            .replace(u'+0000', u'')

        if len(parts) > 5 and parts[5].startswith(u'content='):
            parts[5] = u'content=' + str(len(parts[5]) - 10)

        if len(parts) > 6:
            parts[6] = parts[6].replace(u'\0', u'\\0')

        dst.write(u'\t'.join(parts) + u'\n')

    def generateGraphData(self, skipParsing=False):
        stats = smsgraphs.Stats(self.combinedFilePath, self.pathGraphs, self.statsFilePath, self.settings.partnerMap,
                               self.settings.partnerDirMap, self.settings.salt)
        if not skipParsing:
            safePrint(u'\nParsing data')
            stats.process()
            stats.pickle()
        else:
            safePrint(u'Loading parsed data')
            stats.unpickle()

        safePrint(u'Generating data files to %s' % self.pathGraphs)
        # stats.dumpStats()
        stats.createGraphs()

    def run(self):
        if self.settings.enableDownload:
            self.download()
        files = os.listdir(self.pathLogs)
        files = itertools.chain([os.path.join('pc', f) for f in os.listdir(os.path.join(self.pathLogs, 'pc'))],
                                files)
        self.combineDataFiles(files)
        self.generateGraphData()


if __name__ == "__main__":
    SmsLogProcessor().safeRun()
