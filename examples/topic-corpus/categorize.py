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
from collections import Counter
from pprint import pprint
from random import Random

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


def categorize(schema, text, n_categories=5, n_terms=30):
    """Categorize a piece of text using a MoreLikeThis query on Solr

    This is basically an approximated k-Neareast Neighbors using the TF-IDF
    similarity of the Solr index. The query is truncated to the top n_terms
    terms with maximum weights for efficiency reasons.
    """
    q = MoreLikeThisDocument(text)
    solr = sunburnt.SolrInterface("http://localhost:8983/solr", schema)

    # TODO: add support for the MoreLikeThisHandler instead to avoid useless
    # indexing and deletions
    # https://github.com/tow/sunburnt/issues/18
    solr.add(q)
    solr.commit()
    try:
        mlt_query = solr.query(id=q.id).mlt(
            "text", maxqt=n_terms, count=n_categories)
        mlt_results = mlt_query.execute().more_like_these
        if q.id in mlt_results:
            return [d['id'] for d in mlt_results[q.id].docs]
        else:
            print "ERROR: query document with id='%s' not found" % q.id
            return []
    finally:
        solr.delete(q)
        solr.commit()

def bagging_categorize(schema, text, n_categories=5, n_bootstraps=5, seed=42):
    """Bootstrap aggregating version of the kNN categorization"""
    tokens = text.split()
    bigrams = [" ".join(tokens[i: i + 1]) for i in range(len(tokens))]
    rng = Random(seed)
    bootstrap_size = 2 * len(bigrams) / 3

    categories = []
    for i in range(n_bootstraps):
        doc = u" ".join(rng.sample(bigrams, bootstrap_size))
        categories.extend(categorize(schema, doc, n_categories * 2))

    counted_categories = Counter(categories)
    return [cat for cat, c in counted_categories.most_common(n_categories)
            if c > 2 * n_bootstraps / 3]


if __name__ == "__main__":

    schema = sys.argv[1]
    document = sys.argv[2]
    if document.startswith("http://"):
        document = fetch_text_from_url(document)
    elif os.path.exists(document):
        document = open(document).read()

    for topic in bagging_categorize(schema, document):
        print topic
