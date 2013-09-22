#!/usr/bin/env python

__author__ = "David Stainton"

# external imports
import argparse
import sys
import os
import subprocess
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado import ioloop
from tornado import gen
import tornado.concurrent
import re
import itertools


def getCmd(cmd, verbose=False):
    if verbose:
        print cmd
    output = read_cmd(cmd)
    if verbose:
        print output
    return output

def read_cmd(cmd,input=None,cwd=None):
    """Run the given command and return its output string"""

    pipe = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=cwd)
    # communicate performs the wait for us
    (out,err) = pipe.communicate( input )
    returncode = pipe.poll()
    if returncode == os.EX_OK:
        return out
    else:
        if sys.stderr != None:
            sys.stderr.write(err)
        raise Exception( "read_cmd: %s exited with %s" % (cmd, returncode) )

def isDeployLine(line):
    return re.match('^v\d+\s+Deploy', line) is not None

def heroku_get_last_releases(app, last_num):
    """Retrieve the last_num number of the latest release git commit IDs."""

    cmd = "heroku releases --app %s" % (app,)
    lines = getCmd(cmd).splitlines()
    deploys = itertools.ifilter(isDeployLine, lines)

    latest_releases = []
    for i in range(last_num):
        latest_releases.append(deploys.next().split()[2])

    return latest_releases

def isFileChangeLine(line):
    return re.match('^[MD]\s+', line) is not None

def git_files_changed(oldcommit, newcommit):
    """Return a list of files modified between the two git commits."""
    cmd = "git log --name-status %s..%s" % (oldcommit, newcommit)
    lines = getCmd(cmd).splitlines()
    changes = itertools.ifilter(isFileChangeLine, lines)
    return [line.split()[1] for line in changes]

class FastlyCachePurge():
    """This class is used to asynchronously purge files from the Fastly CDN cache via their HTTP API."""
    def __init__(self, api_key=None, service_id=None):
        self.api_key         = api_key
        self.service_id      = service_id
        self.http_client     = None

    @gen.coroutine
    def async_purge(self, files=None, max_concurrency=None, isVerbose=False):
        """Asynchronously purge all the files from the Fastly CDN cache."""
        self.files               = files
        self.isVerbose           = isVerbose

        self.http_client = AsyncHTTPClient()
        AsyncHTTPClient.configure(None, max_clients=max_concurrency)

        workers = []
        for i in range(min(len(self.files), max_concurrency)):
            workers.append(self.purge_worker())
        yield workers

    @gen.coroutine
    def purge_worker(self):
        while self.files:
            file = self.files.pop()
            yield self.fastly_purge_file(file)

    @gen.coroutine
    def fastly_purge_file(self, file):
        """This asynchronous function tells the Fastly API to purge a file from cache.

        The coroutine decorator turns this function into a generator which yields a future object."""

        headers     = {'Fastly-Key':self.api_key}
        request_url = "https://api.fastly.com/service/%s/purge/%s" % (self.service_id, file)
        myRequest   = HTTPRequest(url     = request_url,
                                  method  = 'POST',
                                  headers = headers,
                                  body    = '')
        try:
            print "http fetch: %s" % myRequest.url
            #response = yield self.http_client.fetch(myRequest)
        except tornado.httpclient.HTTPError as error:
            print "HTTP fail with non-200 code: %s" % error.response
            print "error response body: %s" % error.response.body
            sys.exit(1)


@gen.coroutine
def main():
    """Fastly purge CDN cache.

    This is crash only software and as such should crash early.
    We attempt to purge all the files asynchronously but if there is any kind of failure the program should immediately exit.
    There is no retrying or self healing; just total success or fail.

    Get the last two commit IDs from a git repo and find generate a list of files
    that were modified between them. Purge these files from Fastly CDN.
    """
    parser     = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', dest='isVerbose', default=False, action="store_true", help="Verbose output.")
    parser.add_argument("--heroku-dest", dest='heroku_app_git', help="Heroku git repo for the app.")
    parser.add_argument("--heroku-app", dest='heroku_app', help="Heroku app instance name.")
    parser.add_argument("--api-key", dest='api_key', help="Fastly API key")
    parser.add_argument("--service-id", dest='service_id', help="Fastly service ID")
    parser.add_argument("--max-concurrency", dest='max_concurrency', type=int, default=10, help="Max async HTTP concurrency.")
    args       = parser.parse_args()


    current_commit,previous_commit = heroku_get_last_releases(args.heroku_app, 2)
    files = git_files_changed(previous_commit, current_commit)

    fastly = FastlyCachePurge(api_key=args.api_key, service_id=args.service_id)
    yield fastly.async_purge(max_concurrency=args.max_concurrency, isVerbose=args.isVerbose, files=files)


if __name__ == '__main__':
    ioloop.IOLoop.instance().run_sync(main)
