#!/bin/sh

WORKSPACE=../../workspace
DBPEDIA_36_EN=http://downloads.dbpedia.org/3.6/en

(cd $WORKSPACE && wget -c $DBPEDIA_36_EN/article_categories_en.nt.bz2)
(cd $WORKSPACE && wget -c $DBPEDIA_36_EN/skos_categories_en.nt.bz2)
(cd $WORKSPACE && wget -c $DBPEDIA_36_EN/long_abstracts_en.nt.bz2)

