from __future__ import print_function
import json
import requests
from requests.structures import CaseInsensitiveDict
import os
import sys

PY3 = sys.version_info[0] == 3
if PY3:
    string_types = str,
else:
    string_types = basestring,

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse


class AttrDict(dict):
    """
    Taken from http://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute-in-python/25320214
    But it seems we should at some point switch to https://pypi.python.org/pypi/attrdict
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class ConsoleLog(object):
    """
    Basic console logger. Most frameworks would probably want to implement their own.
    """

    def __init__(self, verbosity=5):
        self.verbosity = verbosity

    def __call__(self, level, msg):
        """If level is less than or equal to verbosity, prints level and the msg"""
        if self.isEnabled(level):
            print((level, msg))

    def isEnabled(self, level):
        """True if level is less than or equal to verbosity set for this instance"""
        return level <= self.verbosity


class ApiError(Exception):
    """
    Any error reported by the API is included in this exception
    """

    def __init__(self, message, data):
        self.message = message
        self.data = data

    def __str__(self):
        return self.message + ': ' + json.dumps(self.data)


class ApiPagesModifiedError(ApiError):
    """
    This error is thrown by queryPage() if revision of some pages was changed between calls.
    """

    def __init__(self, data):
        super(ApiError, self).__init__('Pages modified during iteration', data)


def parseJson(value):
    if isinstance(value, string_types):
        return json.loads(value, object_hook=AttrDict)
    elif hasattr(value.__class__, 'json'):
        return value.json(object_hook=AttrDict)
    else:
        # Our servers still have requests 0.8.2 ... :(
        return json.loads(value.content, object_hook=AttrDict)


class Site(object):
    """
    Public properties (member variables at the moment):
    * url: Full url to site's api.php
    * session: current request.session object
    * log: an object that will be used for logging. ConsoleLog is created by default
    """

    def __init__(self, url, headers=None, session=None, log=None):
        self._loginOnDemand = False
        self.session = session if session else requests.session()
        self.log = log if log else ConsoleLog()
        self.url = url
        self.tokens = {}
        self.noSSL = False  # For non-ssl sites, it might be needed to avoid HTTPS

        try:
            script = os.path.abspath(sys.modules['__main__'].__file__)
        except (KeyError, AttributeError):
            script = sys.executable
        path, f = os.path.split(script)
        self.headers = CaseInsensitiveDict({u'User-Agent': u'%s-%s BareboneMWReq/0.1' % (os.path.basename(path), f)})
        if headers:
            self.headers.update(headers)

    def __call__(self, action, **kwargs):
        """
            Make an API call with any arguments provided as named values:

                data = site('query', meta='siteinfo')

            By default uses GET request to the default URL set in the Site constructor.
            In case of an error, ApiError exception will be raised
            Any warnings will be logged via the logging interface

            :param action could also be

            Several special "magic" parameters could be used to customize api call.
            Special parameters must be all CAPS to avoid collisions with the server API:
            :param POST: Use POST method when calling server API. Value is ignored.
            :param HTTPS: Force https (ssl) protocol for this request. Value is ignored.
            :param EXTRAS: Any extra parameters as passed to requests' session.request(). Value is a dict()
        """
        # Magic CAPS parameters
        method = 'POST' if 'POST' in kwargs or action in ['login', 'edit'] else 'GET'
        forceSSL = not self.noSSL and (action == 'login' or 'SSL' in kwargs or 'HTTPS' in kwargs)
        request_kw = dict() if 'EXTRAS' not in kwargs else kwargs['EXTRAS']

        # Clean up magic CAPS params as they shouldn't be passed to the server
        for k in ['POST', 'SSL', 'HTTPS', 'EXTRAS']:
            if k in kwargs:
                del kwargs[k]

        for k, val in kwargs.items():
            # Only support the well known types.
            # Everything else should be client's responsibility
            if isinstance(val, list) or isinstance(val, tuple):
                kwargs[k] = '|'.join(val)

        # Make server call
        kwargs['action'] = action
        kwargs['format'] = 'json'

        if method == 'POST':
            request_kw['data'] = kwargs
        else:
            request_kw['params'] = kwargs

        if self._loginOnDemand and action != 'login':
            self.login(self._loginOnDemand[0], self._loginOnDemand[1])

        data = parseJson(self.request(method, forceSSL=forceSSL, **request_kw))

        # Handle success and failure
        if 'error' in data:
            raise ApiError('Server API Error', data['error'])
        if 'warnings' in data:
            self.log(2, data['warnings'])
        return data

    def login(self, user, password, onDemand=False):
        """
        :param user:
        :param password:
        :param onDemand: if True, will postpone login until an actual API request is made
        :return:
        """
        self.tokens = {}
        if onDemand:
            self._loginOnDemand = (user, password)
            return
        res = self('login', lgname=user, lgpassword=password)['login']
        if res['result'] == 'NeedToken':
            res = self('login', lgname=user, lgpassword=password, lgtoken=res['token'])['login']
        if res['result'] != 'Success':
            raise ApiError('Login failed', res)
        self._loginOnDemand = False

    def query(self, **kwargs):
        """
        Call Query API with given parameters, and yield all results returned
        by the server, properly handling result continuation.
        """
        if 'rawcontinue' in kwargs:
            raise ValueError("rawcontinue is not supported with query() function, use object's __call__()")
        if 'continue' not in kwargs:
            kwargs['continue'] = ''
        req = kwargs
        while True:
            result = self('query', **req)
            if 'query' in result:
                yield result['query']
            if 'continue' not in result:
                break
            # re-send all continue values in the next call
            req = kwargs.copy()
            req.update(result['continue'])

    def queryPages(self, **kwargs):
        """
        Query the server and return all page objects individually.
        """
        incomplete = {}
        changed = set()
        for result in self.query(**kwargs):
            if 'pages' not in result:
                raise ApiError('Missing pages element in query result', result)

            finished = incomplete.copy()
            for pageId, page in result['pages'].items():
                if pageId in changed:
                    continue
                if pageId in incomplete:
                    del finished[pageId]  # If server returned it => not finished
                    p = incomplete[pageId]
                    if 'lastrevid' in page and p['lastrevid'] != page['lastrevid']:
                        # someone else modified this page, it must be requested anew separately
                        changed.add(pageId)
                        del incomplete[pageId]
                        continue
                    self._mergePage(p, page)
                else:
                    p = page
                incomplete[pageId] = p
            for pageId, page in finished.items():
                if pageId not in changed:
                    yield page

        for pageId, page in incomplete.items():
            yield page
        if changed:
            # some pages have been changed between api calls, notify caller
            raise ApiPagesModifiedError(list(changed))

    def _mergePage(self, a, b):
        """
        Recursively merge two page objects
        """
        for k in b:
            val = b[k]
            if k in a:
                if isinstance(val, dict):
                    self._mergePage(a[k], val)
                elif isinstance(val, list):
                    a[k] = a[k] + val
                else:
                    a[k] = val
            else:
                a[k] = val

    def token(self, tokenType='csrf'):
        if tokenType not in self.tokens:
            self.tokens[tokenType] = next(self.query(meta='tokens', type=tokenType))['tokens'][tokenType + 'token']
        return self.tokens[tokenType]

    def request(self, method, forceSSL=False, headers=None, **request_kw):
        """Make a low level request to the server"""
        url = self.url
        if forceSSL:
            parts = list(urlparse.urlparse(url))
            parts[0] = 'https'
            url = urlparse.urlunparse(parts)
        if headers:
            h = self.headers.copy()
            h.update(headers)
            headers = h
        else:
            headers = self.headers

        r = self.session.request(method, url, headers=headers, **request_kw)
        if not r.ok:
            raise ApiError('Call failed', r)

        if self.log.isEnabled(5):
            dbg = [r.request.url, headers]
            self.log(5, dbg)
        return r


def wikimedia(language='en', site='wikipedia', scheme='https', session=None, log=None):
    """Create a Site object for Wikimedia Foundation site in this format:
        [scheme]://[language].[site].org/w/api.php
    """
    return Site(scheme + '://' + language + '.' + site + '.org/w/api.php', session, log)


if __name__ == '__main__':
    w = wikimedia()
    # r = w.query(meta='siteinfo')
    for v in w.queryPages(titles=('Test', 'API'), prop=('links', 'info'), pllimit=20):
        print(v)
    print('end')
