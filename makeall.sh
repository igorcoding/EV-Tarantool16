#!/usr/bin/env bash

MODULE=`perl -ne 'print($1),exit if m{version_from.+?([\w/.]+)}i' Makefile.PL`;
perl=perl
$perl -v

rm -rf dist && mkdir dist
rm -rf MANIFEST.bak Makefile.old MYMETA.* META.* && \
AUTHOR=1 $perl Makefile.PL && \
make manifest && \
cp MYMETA.yml META.yml && \
cp MYMETA.json META.json && \
make && \
#TEST_AUTHOR=1 make test && \
#TEST_AUTHOR=1 runprove 'xt/*.t' && \
make disttest && \
make dist && \
mv -f *.tar.gz dist/ && \
make clean && \
cp META.yml MYMETA.yml && \
cp META.json MYMETA.json && \
rm -rf MANIFEST.bak Makefile.old && \
echo "All is OK"
