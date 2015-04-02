import csv
from datetime import datetime
import io
import json
import os
import traceback

from unidecode import unidecode

from api import AttrDict
import api
from utils import CsvUnicodeWriter, CsvUnicodeReader


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


def joinValues(vals):
    return u','.join([unicode(v) for v in vals])


def writeData(filename, data, header, delimiter='\t'):
    colCount = len(header)
    tmpFile = filename + '.tmp'
    with CsvUnicodeWriter(tmpFile, csv.excel, delimiter=delimiter) as out:
        out.writerow(header)
        for vals in data:
            if 0 < colCount != len(vals):
                raise ValueError(u'Value should have %d columns, not %d for file %s\n%s' %
                                 (colCount, len(vals), filename, joinValues(vals)))
            out.writerow([unicode(v) for v in vals])
    if os.path.exists(filename):
        os.remove(filename)
    os.rename(tmpFile, filename)


def readData(filename, colCount=0, delimiter='\t'):
    """
    :type filename str|unicode
    :type colCount int|list
    :type separator str|unidecode:
    :return:
    """
    if type(colCount) is list:
        colCount = len(colCount)
    skipFirst = colCount > 0
    if not skipFirst:
        colCount = -colCount
    with CsvUnicodeReader(filename, delimiter=delimiter) as inp:
        for vals in inp:
            if 0 < colCount != len(vals):
                raise ValueError('This value should have %d columns, not %d: %s in file %s' %
                                 (colCount, len(vals), joinValues(vals), filename))
            if skipFirst:
                skipFirst = False
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


class ScriptProcessor(object):
    def __init__(self, settingsFile, pathSuffix):

        self._wiki = None
        self.dateFormat = '%Y-%m-%d'
        self.dateTimeFormat = '%Y-%m-%d %H:%M:%S'

        self.settingsFile = self.normalizePath(settingsFile, False)

        settings = self.defaultSettings(pathSuffix)
        if os.path.isfile(self.settingsFile):
            with io.open(self.settingsFile, 'rb') as f:
                settings = update(json.load(f, object_hook=AttrDict), settings)
        else:
            safePrint('Settings file does not exist, creating default ' + self.settingsFile)
        self.settings = settings
        self.onSettingsLoaded()

        if not self.settings.proxy or not self.settings.proxyPort:
            if self.settings.proxy or self.settings.proxyPort:
                safePrint(u'\nIgnoring proxy settings - both proxy and proxyPort need to be set')
            self.proxy = self.proxyPort = self.proxyUrl = None
        else:
            self.proxy = self.settings.proxy
            self.proxyPort = self.settings.proxyPort
            self.proxyUrl = {'http': 'http://%s:%d' % (self.proxy, self.proxyPort)}


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
        if not path:
            return False
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
        s = AttrDict()
        s.apiUrl = 'https://zero.wikimedia.org/w/api.php'
        s.apiUsername = ''
        s.apiPassword = ''
        s.lastErrorMsg = ''
        s.lastErrorTs = False
        s.lastGoodRunTs = False
        s.smtpFrom = False
        s.smtpHost = False
        s.smtpTo = False
        s.proxy = False
        s.proxyPort = 0
        return s

    def onSavingSettings(self):
        s = self.settings
        s.lastErrorTs = self.formatDate(s.lastErrorTs, self.dateTimeFormat)
        s.lastGoodRunTs = self.formatDate(s.lastGoodRunTs, self.dateTimeFormat)

    def onSettingsLoaded(self):
        pass

    def getWiki(self):
        if not self._wiki:
            self._wiki = api.Site(self.settings.apiUrl)
            if self.proxy:
                self._wiki.session.proxies = {'http': 'http://%s:%d' % (self.proxy, self.proxyPort)}
            self._wiki.login(self.settings.apiUsername, self.settings.apiPassword, onDemand=True)
        return self._wiki

    # noinspection PyMethodMayBeStatic
    def formatDate(self, value, dateFormat):
        return value.strftime(dateFormat) if isinstance(value, datetime) else value

    # noinspection PyMethodMayBeStatic
    def parseDate(self, value, dateFormat):
        return datetime.strptime(str(value), dateFormat) if isinstance(value, basestring) else value

    def safeRun(self):
        # noinspection PyBroadException
        try:
            self.saveSettings() # Ensure the file exists from the start
            self.run()
            self.settings.lastGoodRunTs = datetime.now()
        except:
            self.error(traceback.format_exc())
        self.saveSettings()

    def run(self):
        pass


class LogProcessor(ScriptProcessor):
    def __init__(self, settingsFile, pathSuffix):
        super(LogProcessor, self).__init__(settingsFile, pathSuffix)

        if not self.settings.pathLogs or not self.settings.pathCache:
            raise ValueError('One of the paths is not set, check %s' % settingsFile)

        self.pathLogs = self.normalizePath(self.settings.pathLogs)
        self.pathCache = self.normalizePath(self.settings.pathCache)

    def defaultSettings(self, suffix):
        s = super(LogProcessor, self).defaultSettings(suffix)
        if suffix:
            suffix = suffix.strip('/\\')
        suffix = os.sep + suffix if suffix else ''
        s.pathLogs = 'logs' + suffix
        s.pathCache = 'cache' + suffix
        return s
