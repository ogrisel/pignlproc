"""Simple script to perform a similarity query on a Solr index of topic

Solr is expected to run with the provided schema.xml (just use the
`example/` folder of the default Solr distrib with this schema).


You can install sunburnt (the solr / python connector) and lxml with::

    $ pip install sunburnt lxml

"""

import os
import sys
import urllib2
import uuid

import sunburnt
import lxml.html
from lxml.etree import ElementTree


class MoreLikeThisDocument(object):
    """Transient document to get indexed to be able to do a similarity query"""

    def __init__(self, text):
        self.id = uuid.uuid4().get_hex()
        self.type = "mlt_query_document"
        self.text = text


def fetch_text_from_url(url):
    """Simple helper to scrap the text content of a webpage"""
    opener = urllib2.build_opener()
    request = urllib2.Request(url)
    # change the User Agent to avoid being blocked by Wikipedia
    # downloading a couple of articles ones should not be abusive
    request.add_header('User-Agent', 'pignlproc categorizer')
    html_content = opener.open(request).read()
    tree = ElementTree(lxml.html.document_fromstring(html_content))
    elements = [e.text_content()
                for tag in ('h1', 'h2', 'h3', 'h4', 'p')
                for e in tree.findall('//' + tag)]
    text = "\n\n".join(elements)
    return text


def categorize(schema, text):
    """Categorize a piece of text using a MoreLikeThis query on Solr"""
    q = MoreLikeThisDocument(text)
    solr = sunburnt.SolrInterface("http://localhost:8983/solr", schema)

    # TODO: add support for the MoreLikeThisHandler instead to avoid useless
    # indexing and deletions
    # https://github.com/tow/sunburnt/issues/18
    solr.add(q)
    solr.commit()
    try:
        mlt_query = solr.query(id=q.id).mlt("text")
        mlt_results = mlt_query.execute().more_like_these
        if q.id in mlt_results:
            return [d['id'] for d in mlt_results[q.id].docs]
        else:
            print "ERROR: query document with id='%s' not found" % q.id
            return []
    finally:
        solr.delete(q)
        solr.commit()


if __name__ == "__main__":

    schema = sys.argv[1]
    document = sys.argv[2]
    if document.startswith("http://"):
        document = fetch_text_from_url(document)
    elif os.path.exists(document):
        document = open(document).read()

    for topic in categorize(schema, document):
        print topic.replace('db:', 'http://dbpedia.org/resource/')
