# Building a universal document classifier with Solr MoreLikeThis queries

This folder provides basic Apache Pig scripts and Apache Solr configuration to
build a universal text document classifier based on the taxonomy of Wikipedia
categories that has been extracted as RDF dumps (using the SKOS vocabulary) by
the DBpedia crew.

Categorization can then be achieved by using the `MoreLikeThisHandler` that is
able to perform text similarity / relatedness queries. A sample client able to
perform such queries is provided in the `categorize.py` python script.


## Merge the abstracts of all articles belonging to identical categories

- Clone this repo and build pignlproc with `mvn assembly:assembly` (obviously
  you need a JDK and maven installed)

- Install <http://pig.apache.org> and put the `bin/` folder in your PATH to have
  the `pig` command ready:

      <pre><code>
      pig --version
      Apache Pig version 0.8.1 (r1094835)
      compiled Apr 18 2011, 19:26:53
      </code></pre>

- Download the following dumps file from <http://wiki.dbpedia.org/Downloads36>:

  - article_categories_en.nt.bz2
  - skos_categories_en.nt.bz2
  - long_abstracts_en.nt.bz2
  - infobox_properties_en.nt.bz2

  and put those files in the `workspace/` subfolder of `pignlproc`.

- Run the scripts from the toplevel folder of `pignlproc`
  (this can take more than 1h on a single machine):

      pig -x local -b examples/topic-corpus/01_count_child_topics.pig
      pig -x local -b examples/topic-corpus/02_find_grounded_topics.pig
      pig -x local -b examples/topic-corpus/03bis_extract_aggregate_topic_abstracts.pig

The interesting resulting file is `workspace/topics_abstracts.tsv` and should be
around 500MB.


## Setup Solr

- Download Apache Solr from the official mirrors at <http://lucene.apache.org>

- Assuming you unarchived it in `/opt`, replace the default `schema.xml` with
  the one provided by this example in the folder:

      /opt/apache-solr-3.1.0/example/solr/conf/

- In the same folder, in the `solrconfig.xml` file, insert the following
  handler declaration (after the other handler declarations for instance):

      <requestHandler name="/mlt" class="solr.MoreLikeThisHandler" startup="lazy" />

- Start the solr example server instance:

      cd /opt/apache-solr-3.1.0/example/
      java -Xmx2g -jar start.jar

- Solr should start on <http://localhost:8983/solr/>


## Index the topics aggregate text

We can now launching the indexing itself using the CSV / TSV importer of Solr
(adjust the path in the `stream.file` query parameter):

    curl 'http://localhost:8983/solr/update/csv?commit=true&separator=%09&headers=false&fieldnames=id,popularity,text&stream.file=/path/to/topics_abstracts.tsv&stream.contentType=text/plain;charset=utf-8'


## Performing queries

Install sunburnt (e.g. the development version directly from github with pip)

    sudo pip install https://github.com/tow/sunburnt

Then, from this folder, run:

    python categorize.py schema.xml http://lucene.apache.org/
    Category:Apache_Software_Foundation
    Category:Continuous_integration
    Category:Message-oriented_middleware
    Category:Red_Hat
    Category:Windows_95

As we can see the categories found using this strategy are not yet
great. Ways to improve it would be to follow the SKOS hierarchy to find
the more generic categories using individual subcategories as voters.


## Using Apache Stanbol

You might have noticed that the last pig script outputs a NTriples
variants of the `topic_abtracts.tsv` file. This RDF version is
suitable for indexing by the EntityHub component of Apache Stanbol
(incubating). Enhancement engines deployed in Stanbol should hence be
able to leverage the embedded Solr server of the EntityHub to do text
document categorization and output the results in a structured format
such as JSON-LD or RDF/XML for instance.

More to come later on <http://incubator.apache.org/stanbol>.
