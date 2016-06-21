#! /bin/sh
echo '/static/d' > il_filt
grep static $1 | awk '{ print "s/opcode[.]"$4"/"$6}' | sed -e 's.;./.' | sort -r >> il_filt
