# pignlproc

Apache Pig utilities to build training corpora for machine learning /
NLP out of public Wikipedia and DBpedia dumps.

## Project status

This project is alpha / experimental code. Features are implemented when needed.

Some preliminary results are available in this blog post:

  * [Mining Wikipedia with Hadoop and Pig for Natural Language Processing](http://www.nuxeo.com/blog/mining-wikipedia-with-hadoop-and-pig-for-natural-language-processing/)

## Building from source

Install maven (tested with 3.3.9) and java jdk >= 6, then:

    $ mvn package

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

The [pignlproc wiki](https://github.com/ogrisel/pignlproc/wiki) provides
comprehensive documentation on where to download the dumps from and how
to setup a Hadoop cluster on EC2 using [Apache Whirr](
http://incubator.apache.org/whirr).

It is compatible with AWS EMR 5.2.1.

### Extracting links from a raw Wikipedia XML dump

You can take example on the extract-links.pig example script:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p LANG=fr \
      -p INPUT=src/test/resources/frwiki-20101103-pages-articles-sample.xml \
      -p OUTPUT=/tmp/output \
      examples/extract_links.pig

### Building a NER training / evaluation corpus from Wikipedia and DBpedia

The goal of those samples scripts is to extract a pre-formatted corpus
suitable for the training of sequence labeling algorithms such as MaxEnt
or CRF models with [OpenNLP](http://incubator.apache.org/opennlp),
[Mallet](http://mallet.cs.umass.edu/) or
[crfsuite](http://www.chokkan.org/software/crfsuite/).

To achieve this you can run time following scripts (splitted into somewhat
independant parts that store intermediate results to avoid recomputing
everything from scratch when you can the source files or some parameters.

The first script parses a wikipedia dump and extract occurrences of
sentences with outgoing links along with some ordering and positioning
information:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p LANG=en \
      -p INPUT=src/test/resources/enwiki-20090902-pages-articles-sample.xml \
      -p OUTPUT=workspace \
      examples/ner-corpus/01_extract_sentences_with_links.pig

The parser has been measured to run at a processing of 1MB/s on in local
mode on a MacBook Pro of 2009.

The second script parses dbpedia dumps assumed to be in the folder
/home/ogrisel/data/dbpedia:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p LANG=en \
      -p INPUT=/home/ogrisel/data/dbpedia \
      -p OUTPUT=workspace \
      examples/ner-corpus/02_dbpedia_article_types.pig

This step should complete in a couple of minutes in local mode.

This script could be adapted / replaced to use other typed entities
knowledge bases linked to Wikipedia with downloadable dumps in NT
or TSV formats; for instance: [freebase](http://freebase.com) or
[Uberblic](http://uberblic.org).

The third script merges the partial results of the first two scripts and
order back the results by grouping the sentences of the same article
together to be able to build annotated sentences suitable for OpenNLP
for instance:

    $ pig -x local \
      -p PIGNLPROC_JAR=target/pignlproc-0.1.0-SNAPSHOT.jar \
      -p INPUT=workspace \
      -p OUTPUT=workspace \
      -p LANG=en \
      -p TYPE_URI=http://dbpedia.org/ontology/Person \
      -p TYPE_NAME=person \
      examples/ner-corpus/03bis_filter_join_by_type_and_convert.pig

    $ head -3 workspace/opennlp_person/part-r-00000
    The Table Talk of <START:person> Martin Luther <END> contains the story of a 12-year-old boy who may have been severely autistic .
    The New Latin word autismus ( English translation autism ) was coined by the Swiss psychiatrist <START:person> Eugen Bleuler <END> in 1910 as he was defining symptoms of schizophrenia .
    Noted autistic <START:person> Temple Grandin <END> described her inability to understand the social communication of neurotypicals , or people with normal neural development , as leaving her feeling "like an anthropologist on Mars " .


### Building a document classification corpus

TODO: Explain howto extract bag of words or ngrams and document frequency
features suitable for document classification using a SGD model from
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

