'''S3:// Style URL support for pip

'''
import urlparse

try:
    import boto
except ImportError:
    boto = None

from pip.log import logger

def get_bucket(bucket):
    if boto is None:
        logger.fatal('Unable to import boto, boto is needed for s3:// URL suppport. Try pip install boto to install it.')
        raise ImportError('boto')
    try:
        # Authentication is deferred to boto defaults (environment
        # variables, ~/.boto, /etc/boto or IAM roles on AWS
        # instances).
        s3 = boto.connect_s3()
        return s3.get_bucket(bucket)
    except boto.exception.NoAuthHandlerFound, e:
        logger.fatal('Failed to authenticate. Please configure a source of credentials according to http://boto.readthedocs.org/en/latest/boto_config_tut.html#credentials. %s' % e)
        raise
    except Exception, e:
        logger.fatal('Problem talking to S3: %s' % e)
        raise

def get_url_for_s3_path(url):
    scheme, bucketname, path, query, fragment = urlparse.urlsplit(url)
    bucket = get_bucket(bucketname)
    try:
        key = bucket.get_key(path)
        if key is None:
            raise IOError('Unable to get key %s from %s' % (key, url))
        new_url = key.generate_url(60)  # 60 second timeout
    except boto.exception.S3ResponseError, e:
        logger.fatal('Unable to fetch key for %s: %s' % (url, e))
        raise
    logger.info('Using {!r} for download of {!r}'.format(new_url, url))
    return new_url

class S3HTMLPageResponse(object):
    def __init__(self, content, headers):
        self.content = content
        self.headers = headers

    def info(self):
        return self.headers

    def read(self):
        return self.content

def list_s3_path(url):
    '''Lists the contents of a given S3 url

    Returns more S3:// urls in a HTML like page

    '''
    scheme, bucketname, path, query, fragment = urlparse.urlsplit(url)
    bucket = get_bucket(bucketname)
    # Strip off any leading /
    if path.startswith('/'):
        path = path[1:]
    try:
        results = []
        for key in bucket.list(prefix=path):
            results.append('<a href="s3://%s/%s">%s</a>' % (bucketname, key.name, key.name))
        return S3HTMLPageResponse("\n".join(results), {"Content-Type": "text/html"})
    except boto.exception.S3ResponseError, e:
        logger.fatal('Unable to list %s: %s' % (url, e))
        raise
