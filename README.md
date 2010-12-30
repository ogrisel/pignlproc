# pignlproc

Apache Pig utilities to build training corpora for machine learning /
NLP out of public Wikipedia and DBpedia dumps.

## Project status

This project is alpha / experimental code. Features are implemented when needed.
Expects bugs and not implemented exceptions.

## Building from source

Install maven (tested with 2.2.1) and java jdk 6, then:

    $ mvn assembly:assembly

This should download the dependencies, build a jar in the target/
subfolder and run the tests.

## Usage

The following introduces some sample scripts to demo the User Defined
Functions provided by pignlproc for some practical Wikipedia mining tasks.

Those examples demo how to use pig on your local machine on sample
files. In production (with complete dumps) you might want to startup a
real Hadoop cluster, upload the dumps into HDFS, adjust the above paths
to match your setup and remove the '-x local' command line parameter to
tell pig to use your Hadoop cluster.

### Extracting links from a raw Wikipedia XML dump

You can take example on the extract-links.pig example script:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=src/test/resources/frwiki-20101103-pages-articles-sample.xml \
      -p OUTPUT=/tmp/output \
      examples/extract-links.pig

### Building a NER training / evaluation corpus from Wikipedia and DBpedia

The goal of those samples scripts is to extract a pre-formatted corpus
suitable for the training of sequence labeling algorithms such as MaxEnt
or CRF models with [OpenNLP](http://incubator.apache.org/opennlp),
[Mallet](http://mallet.cs.umass.edu/) or
[crfsuite](http://www.chokkan.org/software/crfsuite/).

*WARNING*: work in progress

To achieve this you can run time following scripts (splitted into somewhat
independant parts that store intermediate results to avoid recomputing
everything from scratch when you can the source files or some parameters.

The first script parses a wikipedia dump and extract occurrences of
sentences with outgoing links along with some ordering and positioning
information:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=src/test/resources/enwiki-20090902-pages-articles-sample.xml \
      -p OUTPUT=workspace \
      examples/ner-corpus/01-extract-sentences-with-links.pig

The second script parses dbpedia dumps assumed to be in the folder
/home/ogrisel/data/dbpedia:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=/home/ogrisel/data/dbpedia \
      -p OUTPUT=workspace \
      examples/ner-corpus/02-dbpedia-article-types.pig

This script could be adapted / replaced to use other typed entities
knowledge bases linked to Wikipedia with downloadable dumps in NT
or TSV formats; for instance: [freebase](http://freebase.com) or
[Uberblic](http://uberblic.org).

The third script (to be completed) merges the partial results of the
first two scripts and order back the results by grouping the sentences
of the same article together to be able to build annotated sentences
suitable for OpenNLP for instance:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=workspace \
      -p OUTPUT=workspace \
      -p TYPE_URI=http://dbpedia.org/ontology/Person \
      -p TYPE_NAME=person \
      examples/ner-corpus/03-join-sentence-with-types.pig

### Building a document classification corpus

TODO: Explain howto extract bag of words / document frequency features suitable
for document classification using a SGD model from
[Mahout](http://mahout.apache.org) for instance.

## Fetching the data

You can get the latest wikipedia dumps for the english articles here (around
5.4GB compressed, 23 GB uncompressed):

  [enwiki-latest-pages-articles.xml.bz2](http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)

The DBPedia links and entities types datasets are available here:

  [Index of individual DBpedia 3.5.1 dumps](http://wiki.dbpedia.org/Downloads351)

  [Complete multilingual archive](http://downloads.dbpedia.org/3.5.1/all_languages.tar)

All of those datasets are also available from the Amazon cloud as public EBS
volumes:

  [Wikipedia XML dataset EBS Volume](http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2506): <tt>snap-8041f2e9</tt> (all languages - 500GB)

  [DBPedia Triples dataset EBS Volume](http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2319): <tt>snap-63cf3a0a</tt> (all languages - 67GB)

It is planned to have crane based utility function to load them to HDFS
directly from the EBS volume.

## License

Copyright 2010 Nuxeo and contributors:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

