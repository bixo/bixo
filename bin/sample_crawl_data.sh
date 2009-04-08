#!/bin/bash

#
#  "url"
#  "fetch_time"
#  "headers_raw"
#  "content_raw"


BINDIR=`dirname $0`
BASEDIR=`dirname ${BINDIR}`

URL_LIST=`zcat $1 | cut -f1`

TMP_PAGE=/tmp/.page.txt
TMP_HEADERS=/tmp/.headers.txt

rm $2

for URL in $URL_LIST
do
    echo "Fetching: $URL"
    curl -o $TMP_PAGE -D $TMP_HEADERS $URL

    PAGE=`uuencode -m $TMP_PAGE "" | tail -n +2`
    HEADERS=`uuencode -m $TMP_HEADERS "" | tail -n +2`

    echo -e $URL"\t"1239167779"\t"$HEADERS"\t"$PAGE >> $2

done


