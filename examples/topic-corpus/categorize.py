"""Simple script to perform a similarity query on a Solr index of topic

Solr is expected to run with the provided schema.xml (just use the
`example/` folder of the default Solr distrib with this schema).


You can install sunburnt (the solr / python connector) and lxml with::

    $ pip install sunburnt lxml

"""

import os
import urllib2
from urllib import unquote
from collections import Counter
from pprint import pprint
from random import Random

import sunburnt
import lxml.html
from lxml.etree import ElementTree

import argparse


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


def categorize(schema, text, n_categories=5, n_terms=30,
               server='http://localhost:8983/solr', terms=False):
    """Categorize a piece of text using a MoreLikeThis query on Solr

    This is basically an approximated k-Neareast Neighbors using the TF-IDF
    similarity of the Solr index. The query is truncated to the top n_terms
    terms with maximum weights for efficiency reasons.
    """
    solr = sunburnt.SolrInterface(server, schema)
    interestingTerms = 'list' if terms else 'none'
    q = solr.mlt_query("text", content=text, maxqt=n_terms,
                       interestingTerms=interestingTerms)
    q = q.paginate(rows=n_categories)
    q = q.field_limit(score=True, all_fields=True)
    return q.execute()


def bagging_categorize(schema, text, n_categories=5, n_bootstraps=5, seed=42,
                       n_terms=30, server='http://localhost:8983/solr',
                       terms=False):
    """Bootstrap aggregating version of the kNN categorization"""
    tokens = text.split()
    bigrams = [" ".join(tokens[i: i + 1]) for i in range(len(tokens))]
    rng = Random(seed)
    bootstrap_size = 2 * len(bigrams) / 3

    n_categories
    categories = {}
    category_ids = []
    for i in range(n_bootstraps):
        doc = u" ".join(rng.sample(bigrams, bootstrap_size))
        results = categorize(schema, doc, server=server, n_terms=n_terms,
                             terms=terms, n_categories=n_categories * 2)
        categories.update(dict((cat['id'], cat) for cat in results))
        category_ids.extend(cat['id'] for cat in results)

    counted_categories = Counter(category_ids)
    return [categories[id]
            for id, count in counted_categories.most_common(n_categories)
            if count > 2 * n_bootstraps / 3]


def human_readable(category_id):
    category_id = category_id[len("Category:"):]
    category_id = unquote(category_id)
    return category_id.replace('_', ' ')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Categorize text documents using text Wikipedia categories')
    parser.add_argument(
        'document',
        help=('The URL of a HTML page, filename of text file or '
              'the text content of paragraph to categorize'))
    parser.add_argument(
        '--schema', default='schema.xml',
        help='Path to the Solr schema file')
    parser.add_argument(
        '--solr', default='http://localhost:8983/solr',
        help='URL of the Solr HTTP endpoint')
    parser.add_argument(
        '--terms', default=30,
        type=int, help='Number of interesting terms to use for the query')
    parser.add_argument(
        '--print-raw-text', default=False, action="store_true",
        help='Print the text of the document used as a query')
    parser.add_argument(
        '--print-terms', default=False, action="store_true",
        help='Print the selected terms to use for the query')
    parser.add_argument(
        '--print-paths', default=False, action="store_true",
        help='Print the paths of the categories')
    parser.add_argument(
        '--bagging', default=False, action="store_true",
        help='Use the bootstrap aggregating categorizer.')
    parser.add_argument(
        '--categories', default=5,
        type=int, help='Number of categories to return')
    args = parser.parse_args()

    schema = args.schema
    document = args.document
    server = args.solr
    n_categories = args.categories
    n_terms = args.terms
    print_terms = args.print_terms

    if document.startswith("http://"):
        document = fetch_text_from_url(document)
    elif os.path.exists(document):
        document = open(document).read()

    if args.bagging:
        results = bagging_categorize(schema, document, server=server,
                                     n_categories=n_categories,
                                     n_terms=n_terms)
    else:
        results = categorize(schema, document, server=server,
                             n_categories=n_categories,
                             terms=print_terms,
                             n_terms=n_terms)
    for topic in results:
        topic_name = human_readable(topic['id'])
        print topic_name.ljust(50) + " [%0.3f]" % topic['score']

    if args.print_paths:
        paths = set()
        for topic in results:
            paths.update(topic['paths'])
        for path in sorted(paths):
            print " / ".join(human_readable(element)
                             for element in path.split("/")
                             if element.strip())

    if args.print_raw_text:
        print "Source text:"
        print "\"\"\""
        print document
        print "\"\"\""

    if print_terms and not args.bagging:
        print "Interesting terms:"
        pprint(results.interesting_terms)
