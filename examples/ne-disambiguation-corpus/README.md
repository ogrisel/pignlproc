# Build a Solr index for Named Entity disambiguation using occurrence context

This folder provides basic Apache Pig scripts and Apache Solr configuration to
build an index of occurrences of Named Entities (People, Places, Organizations,
Works, Species...) based on sentences in Wikipedia articles with the links
pointing to those entities.

Such an index is useful for building the a statistal profiles of the context of
occurrences of names that can be queried upon using the `MoreLikeThisHandler`
that is able to perform text similarity / relatedness queries.


## Merge the abstracts of all articles belonging to identical categories

- Clone this repo and build pignlproc with `mvn assembly:assembly` (obviously
  you need a JDK and maven installed)

- Install <http://pig.apache.org> and put the `bin/` folder in your PATH to have
  the `pig` command ready:

        pig --version
        Apache Pig version 0.8.1 (r1094835)
        compiled Apr 18 2011, 19:26:53

- Download a dump of the Wikipedia:

  <http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2>

  and put those files in the `workspace/` subfolder of `pignlproc`.

- Run the scripts from the toplevel folder of `pignlproc`
  (this can take more than 1h on a single machine):

        pig -x local -b examples/ner-corpus/01_extract_sentences_with_links.pig
        pig -x local -b examples/ne-disambiguation-corpus/02_merge_occurrence_contexts.pig

The interesting resulting file is `workspace/entity_contexts.tsv/part-00000`.


## Setup Solr

- Download Apache Solr from the official mirrors at <http://lucene.apache.org>

- Assuming you unarchived it in `/opt`, replace the default `schema.xml` with
  the one provided by this example in the folder:

        /opt/apache-solr-3.1.0/example/solr/conf/

- In the same folder, in the `solrconfig.xml` file, insert the following
  handler declaration (after the other handler declarations for instance):

        <requestHandler name="/mlt" class="solr.MoreLikeThisHandler" startup="lazy" />

- Also in the `solrconfig.xml` file, increase the field size limit to 1 million
  characters:

        <maxFieldLength>1000000</maxFieldLength>

- Start the solr example server instance:

        cd /opt/apache-solr-3.1.0/example/
        java -Xmx2g -jar start.jar

- Solr should start on <http://localhost:8983/solr/>


## Index the topics aggregate text

We can now launching the indexing itself using the CSV / TSV importer of Solr
(adjust the path in the `stream.file` query parameter):

    curl 'http://localhost:8983/solr/update/csv?commit=true&separator=%09&headers=false&fieldnames=id,text&stream.file=/path/to/entity_contexts.tsv&stream.contentType=text/plain;charset=utf-8'


## Performing queries

Install sunburnt (e.g. the development version directly from github with pip)

    sudo pip install https://github.com/tow/sunburnt

TODO: write me!