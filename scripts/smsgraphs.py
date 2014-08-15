import io
import json
from operator import itemgetter
import os
from datetime import *
from collections import defaultdict
from itertools import *

# Daily totals -
#
# A. Number of sessions initiated
# B. Number of unique users initiating sessions
# C. Number of sessions cancelled at search box
# D. Number of sessions initiated and sent an SMS response of any non-zero number of SMS's
# E.  Number of sessions initiated and sent an SMS response, and had no option to reply for more
# F.  Number of sessions initiated and sent an SMS response, and were presented with an option to reply for more
# G.  Of the number above ('F'), how many of those opted to receive more information
# H.  Average number of 'reply for more' requests during a single session (from the set of users in 'D')
# I.    Average number of SMS's sent (from the set of users in 'D')
# J.   Total number of SMS's sent (from the set of users in 'D')
#
# Hourly totals for 24-hour daily period
#
# A. Number of sessions initiated
# B. Number of sessions initiated and sent an SMS response of any non-zero number of SMS's
import re
import unicodedata

stateNames = {
    u'start': u'0 start',
    u'titles': u'1 titles',
    u'section-invalid': u'1 wrong title',
    u'section': u'2 sections',
    u'content-invalid': u'2 wrong section',
    u'ussdcontent': u'3 ussd content',
    u'smscontent': u'3 sms content',
    u'smscontent-2': u'3 sms content (2)',
    u'smscontent-3+': u'3 sms content (3+)',
    u'more-no-content': u'4 no-more-content',
    u'more-no-session': u'5 no-more-session',
    u'more-no-session-2': u'5 no-more-session (2)',
    u'more-no-session-3+': u'5 no-more-session (3+)',
    u'newuser': u'new user',
}

goodStates = [
    u'start',
    u'titles',
    u'section',
    u'smscontent',
    u'smscontent-2',
    u'smscontent-3+',
]

# State machine:
#
# start   ->   titles   ->   section -> ussdcontent -> smscontent(+) -> more-no-content
# v              v
# section-invalid  content-invalid
#
# NOTES:
#   Sometimes smscontent appears in the logs before ussdcontent
#
okTransitions = {
    (u'', u'start'),
    (u'start', u'titles'),
    (u'titles', u'section'),
    (u'titles', u'section-invalid'),
    (u'section', u'ussdcontent'),
    (u'section', u'smscontent'),
    (u'section', u'content-invalid'),
    (u'ussdcontent', u'smscontent'),
    (u'smscontent', u'ussdcontent'),
    (u'smscontent', u'smscontent'),
    (u'smscontent', u'more-no-content'),
    (u'smscontent', u'more-no-session'),
    (u'more-no-content', u'more-no-session'),
    (u'more-no-session', u'more-no-session'),
}

multiactions = {u'smscontent', u'more-no-content', u'more-no-session'}

entrySpecials = {'id', 'ts', 'partner'}


class Entry(object):
    def __init__(self, userId, ts, partner):
        self.id = userId
        self.ts = ts
        self.partner = partner

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def entryItems(self):
        for v in self.__dict__.items():
            if v[0] not in entrySpecials:
                yield v

    def __repr__(self):
        return repr(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __delitem__(self, key):
        del self.__dict__[key]

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def __cmp__(self, d):
        return cmp(self.__dict__, d)

    def __contains__(self, item):
        return item in self.__dict__

    def add(self, key, value):
        self.__dict__[key] = value

    def __call__(self):
        return self.__dict__

    def __unicode__(self):
        return unicode(repr(self.__dict__))

    def items(self):
        return self.__dict__.items()


# class SumEntryEncoder(json.JSONEncoder):
#     def default(self, o):
#         return super(SumEntryEncoder, self).default(o)


class SumEntry(object):
    def __init__(self, value=-1):
        """
        :type value: dict|string|False
        """
        if isinstance(value, dict):
            self.__dict__ = value
        else:
            self.count = 1
            if value >= 0:
                self.sum = value
                self.min = value
                self.max = value

    def addValue(self, value):
        self.count += 1
        if value >= 0:
            self.sum += value
            if self.min > value:
                self.min = value
            if self.max < value:
                self.max = value

    def countOnly(self):
        return 'sum' not in self.__dict__


def splitKey(key):
    isError = key.startswith(u'err-')
    if isError:
        key = key[len(u'err-'):]

    if key.endswith(u'_unique'):
        key = key[0:-len(u'_unique')]
        tp = u'unique'
    elif key.endswith(u'_usrmonth'):
        key = key[0:-len(u'_usrmonth')]
        tp = u'usrmonth'
    elif key == u'newuser':
        tp = key
    else:
        tp = u'total'

    return isError, key, tp


def filterData(data, isError=False, isNewUser=False, isUnique=False, knownState=True, includeStats=False,
               yieldTuple=False):
    for key, dates in data.items():
        isErr, state, typ = splitKey(key)
        if isErr != isError:
            continue
        if (typ == u'unique') != isUnique:
            continue
        if (typ == u'newuser') != isNewUser:
            continue
        if (state in stateNames) != knownState:
            continue
        state = stateNames[state]
        for dateStr, e in dates.items():
            if not dateStr.startswith(u'daily_'):
                continue
            ts = dateStr[len(u'daily_'):]
            if yieldTuple:
                yield (ts, state, e.count) if not includeStats else (ts, state, e.count, e.min, e.avg, e.max)
            else:
                res = {
                    u'date': ts,
                    u'state': state,
                    u'count': e.count,
                }
                if includeStats:
                    res[u'avg'] = e.avg
                    res[u'min'] = e.min
                    res[u'max'] = e.max
                yield res


def createStatesGraph(partnerDir, data, states):
    d = sorted(filterData(data, yieldTuple=True),
               key=lambda v:
               v[0] + v[1])
    # v[u'date'] + v[u'state'])
    # groups = groupby(d, key=itemgetter(''))
    #     [{'type':k, 'items':[x[0] for x in v]} for k, v in groups]
    # from itertools import groupby, islice
    # from operator import itemgetter
    # from collections import defaultdict

    # probably splitting this up in multiple lines would be more readable
    pivot = (
        (
            ts,
            defaultdict(lambda: '', (islice(d, 1, None) for d in dd))
        )
        for ts, dd in groupby(d, itemgetter(0)))

    resultFile = os.path.join(partnerDir, 'states-count-per-day.tsv')
    with io.open(resultFile, 'w', encoding='utf8') as f:
        f.write(u'date\t' + u'\t'.join(states) + u'\n')
        for ts, counts in pivot:
            f.write(ts + u'\t' + u'\t'.join(str(counts[s]) for s in states) + u'\n')


class Stats(object):
    def __init__(self, sourceFile, graphDir, stateFile, partnerMap=None, partnerDirMap=None, salt=''):
        self.newUserUnique = set()
        self.unique = defaultdict(dict)
        self.sourceFile = sourceFile
        self.graphDir = graphDir
        self.stateFile = stateFile
        self.partnerMap = partnerMap if partnerMap is not None else {}
        self.partnerDirMap = partnerDirMap if partnerDirMap is not None else {}
        self.salt = salt
        self.stats = {}

    def _addStats(self, partner, stage, key2, value=-1):
        p = partner if partner is not None else 'allpartners'
        try:
            partnerStat = self.stats[p]
        except KeyError:
            partnerStat = dict()
            self.stats[p] = partnerStat
        try:
            stat = partnerStat[stage]
        except KeyError:
            stat = dict()
            partnerStat[stage] = stat
        if key2 not in stat:
            stat[key2] = SumEntry(value)
        else:
            stat[key2].addValue(value)
        # two-stage addition - one for partner, one total
        if partner is not None:
            self._addStats(None, stage, key2, value)

    def _cleanupStats(self):
        del self.unique
        del self.newUserUnique
        for partner, partnerData in self.stats.items():
            if partner == 'allpartners':
                continue
            for k in list([i for i in partnerData if u'_usrmonth_' in i]):
                kk = k.split(u'_', 2)
                # removing top 1% of the heavy users
                vals = list(sorted(partnerData[k].values(), key=lambda l: l.count))
                vals = vals[0:len(vals) - int(len(vals) * 0.01)]
                for v in vals:
                    self._addStats(partner, kk[0] + u'_' + kk[1], kk[1] + u'_' + kk[2], v.count)
                del partnerData[k]
                if k in self.stats['allpartners']:
                    del self.stats['allpartners'][k]

    def _addStatsUniqueUser(self, partner, key2, userId):
        if userId not in self.newUserUnique:
            self.newUserUnique.add(userId)
            self._addStats(partner, u'newuser', key2)

    def _addStatsUnique(self, partner, stage, key2, userId):
        u = self.unique[stage]
        if key2 not in u:
            u[key2] = {userId}
        elif userId not in u[key2]:
            u[key2].add(userId)
        else:
            return
        self._addStats(partner, stage, key2)
    #        if not isError:
    #            key2 = u'hourly_' + ts.strftime(u'%Y-%m-%d %H') + u':00'
    #            self._addStats(partner, key, key2, value)

    def addStats(self, partner, stage, ts, userId, value=-1):
        #        self._addStats(partner, key, u'_totals', value)
        #        self._addStatsUnique(partner, stage + u'_unique', u'_totals', id)

        key2 = u'daily_' + ts.strftime(u'%Y-%m-%d')
        self._addStats(partner, stage, key2, value)
        self._addStatsUnique(partner, stage + u'_unique', key2, userId)

        self._addStats(partner, stage + u'_usrmonth_' + ts.strftime(u'%m-%Y'), userId)

    def countStats(self, entry):
        ts = entry.ts
        userId = entry.id
        self.addStats(entry.partner, u'start', ts, userId)
        self._addStatsUniqueUser(entry.partner, u'daily_' + ts.strftime(u'%Y-%m-%d'), userId)
        for k, v in entry.entryItems():
            if type(v) is list:
                maxN = 2
                for i in range(min(len(v), maxN + 1)):
                    self.addStats(entry.partner,
                                  k + (u'' if i == 0 else '-' + str(i + 1) + ('+' if i == maxN else '')),
                                  ts, userId, v[i])
            else:
                self.addStats(entry.partner, k, ts, userId, v)

    def process(self):

        cId = 0
        cTime = 1
        cPartner = 4
        cAction = 5
        cContent = 6

        fErr = io.open(os.path.join(self.graphDir, 'errors.txt'), 'w', encoding='utf8')

        isError = False
        lastAction = u''
        lastLine = u''
        lastParts = False
        entry = None
        for line in io.open(self.sourceFile, encoding='utf8'):
            if line == u'':
                break
            if line == lastLine:
                continue
            lastLine = line

            parts = [v.strip() for v in line.split(u'\t')]
            if len(parts) > cContent and parts[cContent].startswith(u'content='):
                parts[cContent] = u'content=' + str(len(parts[cContent]) - 10) + u'chars'

            action = parts[cAction]
            timestamp = datetime.strptime(parts[cTime], u'%Y-%m-%d %H:%M:%S')
            isNew = entry is None or entry.id != parts[cId] or action == u'start'

            if isNew:
                if entry is not None:
                    self.countStats(entry)
                partnerKey = u'|'.join(parts[cPartner - 2:cPartner + 1])
                if partnerKey in self.partnerMap:
                    partner = self.partnerMap[partnerKey]
                else:
                    if parts[cPartner] == u'':
                        partner = u'-'.join(parts[cPartner - 2:cPartner])
                    else:
                        partner = parts[cPartner]
                    self.partnerMap[partnerKey] = partner
                entry = Entry(parts[cId], timestamp, partner)
                lastParts = parts
                lastAction = u''
                isError = False

            transition = (lastAction, action)
            secondsFromStart = int((timestamp - entry.ts).total_seconds())
            isMultiAction = action in multiactions
            isNewAction = action not in entry

            if isError or transition not in okTransitions or (not isNewAction and not isMultiAction):
                if lastParts:
                    fErr.write(u'\n' + lastParts[0] + u'\n' + (u'\t'.join(lastParts[1:])) + u'\n')
                parts[cPartner] = str(secondsFromStart)
                del parts[cId]
                fErr.write(u'\t'.join(parts) + u'\n')
                self.addStats(entry.partner, u'err--bad-transitions', timestamp, entry.id, secondsFromStart)
                key = (u'err-cont-' if isError else u'err-new-') + transition[0] + u'-' + transition[1]
                self.addStats(entry.partner, key, timestamp, entry.id, secondsFromStart)
                lastParts = False
                isError = True
            elif not isNew:
                # noinspection PyTypeChecker
                lastParts = lastParts + [str(secondsFromStart)] + parts[cAction:]
                if isNewAction:
                    entry[action] = [secondsFromStart] if isMultiAction else secondsFromStart
                else:
                    entry[action].append(secondsFromStart)

            lastAction = action

        if lastParts:
            self.countStats(entry)

        self._cleanupStats()

    def pickle(self):
        with open(self.stateFile, 'wb') as f:
            self.recursiveConvert(self.stats)
            json.dump(self.stats, f, indent=True, sort_keys=True)
            self.recursiveConvert(self.stats)

    def unpickle(self):
        with io.open(self.stateFile, 'rb') as f:
            self.stats = json.load(f)
        self.recursiveConvert(self.stats)

    def recursiveConvert(self, d):
        for (k, v) in d.items():
            if isinstance(v, dict):
                if 'count' in v:
                    d[k] = SumEntry(v)
                else:
                    self.recursiveConvert(v)
            elif isinstance(v, SumEntry):
                d[k] = v.__dict__

    def dumpStats(self):

        with io.open(os.path.join(self.graphDir, 'results-err.txt'), 'w', encoding='utf8') as err, \
                io.open(os.path.join(self.graphDir, 'results-err-totals.txt'), 'w', encoding='utf8') as err_t, \
                io.open(os.path.join(self.graphDir, 'results-totals.txt'), 'w', encoding='utf8') as res_t, \
                io.open(os.path.join(self.graphDir, 'results-daily.txt'), 'w', encoding='utf8') as res_d, \
                io.open(os.path.join(self.graphDir, 'results-hourly.txt'), 'w', encoding='utf8') as res_hr:

            hdr = u'\t'.join(
                [u'distinct', u'cont', u'error', u'frequency', u'timestamp', u'count', u'avg', u'min', u'max']) + u'\n'
            err.write(hdr)
            err_t.write(hdr)
            hdr = u'\t'.join(
                [u'distinct', u'state', u'frequency', u'timestamp', u'count', u'avg', u'min', u'max']) + u'\n'
            res_t.write(hdr)
            res_d.write(hdr)
            res_hr.write(hdr)

            allStats = self.stats['allpartners']
            for k in sorted(allStats):
                v = allStats[k]
                key = k
                isError = key.startswith(u'err-')
                if key.endswith(u'_unique'):
                    key = key[0:-len(u'_unique')]
                    line = u'unique'
                elif key.endswith(u'_usrmonth'):
                    key = key[0:-len(u'_usrmonth')]
                    line = u'usrmonth'
                elif key == u'newuser':
                    line = key
                else:
                    line = u'total'
                line += u'\t'
                if isError:
                    k1, k2, k3 = key.split(u'-', 2)
                    line += k2 + u'\t' + k3
                elif key in stateNames:
                    line += stateNames[key]
                else:
                    line += key
                line += u'\t'

                for kk in sorted(v):
                    vv = v[kk]
                    if kk == u'_totals':
                        l = line + u'total\t\t'
                    else:
                        l = line + kk.replace(u'_', u'\t') + u'\t'
                    if vv.countOnly():
                        l += unicode(vv.count)
                    else:
                        l += u'%d\t%g\t%d\t%d' % (vv.count, vv.sum / vv.count, vv.min, vv.max)

                    if kk.startswith(u'hourly_'):
                        f = err if isError else res_hr
                    elif kk == u'_totals':
                        f = err_t if isError else res_t
                    else:
                        f = err if isError else res_d
                    f.write(l + u'\n')

    def makePartnerDir(self, partner):
        partnerKey = self.partnerDirMap[partner]

        dashboard = os.path.join(self.graphDir, 'dashboards')
        if not os.path.exists(dashboard):
            os.mkdir(dashboard)
        datafiles = os.path.join(self.graphDir, 'datafiles')
        if not os.path.exists(datafiles):
            os.mkdir(datafiles)
        dataDir = os.path.join(datafiles, partnerKey)
        if not os.path.exists(dataDir):
            os.mkdir(dataDir)

        # Create an empty file with the partner's name to easily see who is who
        # From http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
        sanitizedPartner = unicodedata.normalize('NFKD', unicode(partner)).encode('ascii', 'ignore')
        sanitizedPartner = unicode(re.sub('[^\w\s-]', '', sanitizedPartner).strip().lower())
        sanitizedPartner = re.sub('[-\s]+', '-', sanitizedPartner)
        infoFile = os.path.join(dataDir, sanitizedPartner)
        if not os.path.exists(infoFile):
            open(infoFile, 'a').close()

        # Create dashboard
        dashboardFile = os.path.join(dashboard, partnerKey + '.json')
        if not os.path.exists(dashboardFile):
            data = {
                "id": partnerKey,
                "headline": partner,
                # "subhead": "subtitle",
                "tabs": [
                    {
                        "name": "Graphs",
                        "graph_ids": [
                            "http://gp.wmflabs.org/data/datafiles/gp_zero_local/%s/states-count-per-day.tsv"
                            % partnerKey
                        ]
                    }
                ]
            }
            with open(dashboardFile, 'wb') as f:
                json.dump(data, f, indent=True, sort_keys=True)

        return dataDir

    def createGraphs(self):

        states = sorted([stateNames[v] for v in goodStates])

        for partner, data in self.stats.items():
            if partner not in self.partnerDirMap:
                import hashlib

                partnerKey = hashlib.sha224(partner + self.salt).hexdigest()
                self.partnerDirMap[partner] = partnerKey

            partnerDir = self.makePartnerDir(partner)
            createStatesGraph(partnerDir, data, states)


if __name__ == '__main__':
    stats = Stats('state/tmp.tsv', 'graphs', 'state/tmp.json')
    # stats = Stats('state/combined.tsv', 'graphs', 'state/combined.json')

    # stats.process()
    #
    # stats.pickle()

    stats.unpickle()

    stats.createGraphs()

    stats.dumpStats()

    pass