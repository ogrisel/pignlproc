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

## Fetching the data

You can get the latest wikipedia dumps for the english articles here (around
5.4GB compressed, 23 GB uncompressed):

  [enwiki-latest-pages-articles.xml.bz2](http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)

The DBPedia links and entities types datasets are available here:

  [wikipage_en.nt.bz2](http://downloads.dbpedia.org/3.5.1/en/wikipage_en.nt.bz2)

  [instancetype_en.nt.bz2](http://downloads.dbpedia.org/3.5.1/en/instancetype_en.nt.bz2)

  [longabstract_en.nt.bz2](http://downloads.dbpedia.org/3.5.1/en/longabstract_en.nt.bz2)

  [pagelinks_en.nt.bz2](http://downloads.dbpedia.org/3.5.1/en/pagelinks_.nt.bz2)

  [redirect_en.nt.bz2](http://downloads.dbpedia.org/3.5.1/en/redirect_en.nt.bz2)

All of those datasets are also available from the Amazon cloud as public EBS
volumes:

  [Wikipedia XML dataset EBS Volume](http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2506): <tt>snap-8041f2e9</tt> (all languages - 500GB)

  [DBPedia Triples dataset EBS Volume](http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2319): <tt>snap-63cf3a0a</tt> (all languages - 67GB)

It is planned to have crane based utility function to load them to HDFS
directly from the EBS volume.

## Usage

### Extracting links from the raw wikipedia dump

You can take example on the extract-links.pig example script:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=src/test/resources/enwiki-20090902-pages-articles-sample.xml \
      -p OUTPUT=/tmp/output \
      examples/extract-links.pig

### Building a NER training / evaluation corpus

TODO: Explain howto extract a BIO-formatted corpus suitable for the
training of sequence labeling algorithms such as MaxEnt or CRF
models with [OpenNLP](http://incubator.apache.org/opennlp),
[Mallet](http://mallet.cs.umass.edu/) or
[crfsuite](http://www.chokkan.org/software/crfsuite/).

Work in progress: assuming the dbpedia dumps are available in
/home/ogrisel/data/dbpedia:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar
      -p DBPEDIA=/home/ogrisel/data/dbpedia \
      -p WIKIPEDIA=src/test/resources/enwiki-20090902-pages-articles-sample.xml \
      -p OUTPUT=/tmp/output \
      examples/build-ner-corpus.pig

### Building a document classification corpus

TODO: Explain howto extract bag of words / document frequency features suitable
for document classification using a SGD model from
[Mahout](http://mahout.apache.org) for instance.

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

