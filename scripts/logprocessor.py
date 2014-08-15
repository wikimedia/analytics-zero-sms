from datetime import datetime
import io
from itertools import chain, imap
import json
import os
import traceback
from unidecode import unidecode
from api import AttrDict

validSites = {
    u'wikipedia',
    u'wikimedia',
    u'wiktionary',
    u'wikisource',
    u'wikibooks',
    u'wikiquote',
    u'mediawiki',
    u'wikimediafoundation',
    u'wikiversity',
    u'wikinews',
    u'wikivoyage',
}


def safePrint(text):
    print(unidecode(unicode(text)))


def joinValues(vals, separator=u'\t', colCount=0):
    if 0 < colCount != len(vals):
        raise ValueError(u'Cannot save value that should have %d columns, not %d\n%s' %
                         (colCount, len(vals), joinValues(vals, u',')))
    return unicode(separator).join([unicode(v) for v in vals])


def writeData(filename, data, columns, separator=u'\t'):
    colCount = len(columns)
    with io.open(filename, 'w', encoding='utf8', errors='ignore') as out:
        out.writelines(
            chain(
                [joinValues(columns, separator) + '\n'],  # header
                imap(lambda vals: joinValues(vals, separator, colCount) + '\n', data)))


def readData(filename, colCount=0, separator=u'\t'):
    """
    :type filename str|unicode
    :type colCount int|list
    :type separator str|unidecode:
    :return:
    """
    if type(colCount) is list:
        colCount = len(colCount)
    isFirst = colCount > 0
    if not isFirst:
        colCount = -colCount
    with io.open(filename, 'r', encoding='utf8', errors='ignore') as inp:
        for line in inp:
            vals = line.strip(u'\r\n').split(separator)
            if 0 < colCount != len(vals):
                raise ValueError('This value should have %d columns, not %d: %s in file %s' %
                                 (colCount, len(vals), joinValues(vals, u','), filename))
            if isFirst:
                isFirst = False
                continue
            yield vals


def update(a, b):
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                update(a[key], b[key])
        else:
            a[key] = b[key]
    return a


class LogProcessor(object):
    def __init__(self, settingsFile, pathSuffix):

        self.dateFormat = '%Y-%m-%d'
        self.dateTimeFormat = '%Y-%m-%d %H:%M:%S'

        self.settingsFile = self.normalizePath(settingsFile, False)

        settings = self.defaultSettings(pathSuffix)
        if os.path.isfile(self.settingsFile):
            with io.open(self.settingsFile, 'rb') as f:
                settings = update(json.load(f, object_hook=AttrDict), settings)
        self.settings = settings
        self.onSettingsLoaded()

        if not self.settings.pathLogs or not self.settings.pathCache or not self.settings.pathGraphs:
            raise ValueError('One of the paths is not set, check %s' % settingsFile)

        self.pathLogs = self.normalizePath(self.settings.pathLogs)
        self.pathCache = self.normalizePath(self.settings.pathCache)
        self.pathGraphs = self.normalizePath(self.settings.pathGraphs)

    def saveSettings(self):
        self.onSavingSettings()
        try:
            filename = self.settingsFile
            backup = filename + '.bak'
            with open(backup, 'wb') as f:
                json.dump(self.settings, f, indent=True, sort_keys=True)
            if os.path.exists(filename):
                os.remove(filename)
            os.rename(backup, filename)
        finally:
            self.onSettingsLoaded()

    def normalizePath(self, path, relToSettings=True):
        if not os.path.isabs(path) and relToSettings:
            path = os.path.join(os.path.dirname(self.settingsFile), path)
        path = os.path.abspath(os.path.normpath(path))
        dirPath = path if relToSettings else os.path.dirname(path)
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        return path

    def error(self, error):
        self.settings.lastErrorTs = datetime.now()
        self.settings.lastErrorMsg = error

        safePrint(error)

        if not self.settings.smtpHost or not self.settings.smtpFrom or not self.settings.smtpTo:
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

        smtp = smtplib.SMTP(self.settings.smtpHost)
        smtp.sendmail(self.settings.smtpFrom, self.settings.smtpTo, msg.as_string().encode('ascii'))
        smtp.quit()

    def defaultSettings(self, suffix):
        if suffix:
            suffix = suffix.strip('/\\')
        suffix = os.sep + suffix if suffix else ''

        s = AttrDict()

        s.lastErrorMsg = ''
        s.lastErrorTs = False
        s.lastGoodRunTs = False

        s.smtpFrom = False
        s.smtpHost = False
        s.smtpTo = False

        s.pathLogs = 'logs' + suffix
        s.pathCache = 'cache' + suffix
        s.pathGraphs = 'graphs' + suffix

        return s

    def onSavingSettings(self):
        s = self.settings
        s.lastErrorTs = self.formatDate(s.lastErrorTs, self.dateTimeFormat)
        s.lastGoodRunTs = self.formatDate(s.lastGoodRunTs, self.dateTimeFormat)

    def onSettingsLoaded(self):
        pass

    # noinspection PyMethodMayBeStatic
    def formatDate(self, value, dateFormat):
        return value.strftime(dateFormat) if isinstance(value, datetime) else value

    # noinspection PyMethodMayBeStatic
    def parseDate(self, value, dateFormat):
        return datetime.strptime(str(value), dateFormat) if isinstance(value, basestring) else value

    def safeRun(self):
        # noinspection PyBroadException
        try:
            self.run()
            self.settings.lastGoodRunTs = datetime.now()
        except:
            self.error(traceback.format_exc())
        self.saveSettings()

    def run(self):
        pass
