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

        pig --version
        Apache Pig version 0.8.1 (r1094835)
        compiled Apr 18 2011, 19:26:53

- Download the following dumps file from <http://wiki.dbpedia.org/Downloads36>:

  - article_categories_en.nt.bz2
  - skos_categories_en.nt.bz2
  - long_abstracts_en.nt.bz2

  and put those files in the `workspace/` subfolder of `pignlproc`. You can use
  the following script under unix:

       sh download_data.sh

- Run the scripts from the toplevel folder of `pignlproc`
  (this can take more than 1h on a single machine):

      pig -x local -b examples/topic-corpus/01_count_child_topics.pig
      pig -x local -b examples/topic-corpus/02_find_grounded_topics.pig
      pig -x local -b examples/topic-corpus/03_find_descendants.pig
      pig -x local -b examples/topic-corpus/04_find_grounded_topics_articles.pig
      pig -x local -b examples/topic-corpus/05_build_grounded_ancestry.pig
      pig -x local -b examples/topic-corpus/06_extract_aggregate_topic_abstracts.pig

In total running those scripts on a single machine would last 1 or 2 hours. It
would be possible to run then on a Hadoop cluster to speed up the processing but
don't expect drammatic speed up on such a smallish corpus.

The interesting resulting file is `workspace/topics_abstracts.tsv` and should be
around 2.5GB.


## Setup Solr

- Download Apache Solr from the official mirrors at <http://lucene.apache.org>

- Assuming you unarchived it in `/opt`, replace the default `schema.xml` with
  the one provided by this example in the folder:

        /opt/apache-solr-3.3.0/example/solr/conf/

- In the same folder, in the `solrconfig.xml` file, insert the following
  handler declaration (after the other handler declarations for instance):

        <requestHandler name="/mlt" class="solr.MoreLikeThisHandler" startup="lazy" />

- Also in the `solrconfig.xml` file, increase the field size limit to a larger
  size thant the default (1000):

        <maxFieldLength>100000</maxFieldLength>

- Start the solr example server instance:

        $ cd /opt/apache-solr-3.3.0/example/ && java -Xmx2g -jar start.jar

- Solr should start on <http://localhost:8983/solr/>


## Index the topics aggregate text

We can now launching the indexing itself using the CSV / TSV importer of Solr
(adjust the path in the `stream.file` query parameter):

    $ curl 'http://localhost:8983/solr/update/csv?commit=true&separator=%09&headers=false&fieldnames=id,popularity,paths,text&stream.file=/path/to/topics_abstracts.tsv/part-r-00000&stream.contentType=text/plain;charset=utf-8'

This process should last less than an hour and eat an additional 5.8GB on your
hardrive.

TODO: Split the paths on the `%20%20` character at import time to get a
multi-valued field.


## Performing queries

Install sunburnt (e.g. the development version directly from github with pip)

    sudo pip install https://github.com/tow/sunburnt

Then from this folder, run for instance:

    $ python categorize.py schema.xml http://lucene.apache.org/
    Category:Open_source_project_foundations
    Category:Search_engine_software
    Category:Free_web_software
    Category:Java_platform_software
    Category:Java_programming_language

    $ python categorize.py schema.xml http://pig.apache.org/
    Category:Compilers
    Category:Open_source_project_foundations
    Category:Software_optimization
    Category:Network_file_systems
    Category:Java_platform_software

TODO: improve the results would be to follow the SKOS hierarchy to find
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

TODO: export the paths information as another `.nt` file.
