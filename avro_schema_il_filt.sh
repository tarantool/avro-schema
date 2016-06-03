#! /bin/sh
grep static $1 | awk '{ print "s/opcode[.]"$4"/"$6}' | sed -e 's.;./.' | sort -r > sedscript
sed -f sedscript -e '/static/d' $1 > $2
