"""Simple script to perform a similarity query on a Solr index of topic

Solr is expected to run with the provided schema.xml (just use the
`example/` folder of the default Solr distrib with this schema).


You can install sunburnt (the solr / python connector) with::

    $ pip install sunburnt

"""

import os
import sys
import uuid
import sunburnt

class MoreLikeThisDocument(object):
    """Transient document to get indexed to be able to do a similarity query"""

    def __init__(self, text):
        self.id = uuid.uuid4().get_hex()
        self.type = "mlt_query_document"
        self.text = text


def categorize(schema, text):
    """Categorize a piece of text using a MoreLikeThis query on Solr"""
    q = MoreLikeThisDocument(text)
    solr = sunburnt.SolrInterface("http://localhost:8983/solr", schema)
    solr.add(q)
    solr.commit()
    try:
        print "query id:", q.id
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
    if os.path.exists(document):
        document = open(document).read()

    for topic in categorize(schema, document):
        print topic
