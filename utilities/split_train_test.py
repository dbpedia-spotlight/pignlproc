#!/usr/bin/env python
"""This script removes n percent of annotated paragraphs from the MediaWiki
dump and writes them to a test file. The training file remains valid XML.

$ bzcat nlwiki-latest-pages-articles.xml.bz2 | python split_train_test.py 0.05 nl_test.txt > nl_train.xml

Author: Joachim Daiber
"""

from __future__ import division
import fileinput
import argparse, sys
import random 
from argparse import RawTextHelpFormatter

parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=RawTextHelpFormatter)

parser.add_argument('test_size', type=int, help='size of the test portion in paragraphs')
parser.add_argument('test_out', help='target file of the test portion')
args = parser.parse_args()

test = open(args.test_out, "w")
in_text = False
p_test = 0

for line in sys.stdin:

    if line.startswith("      <text"):
        in_text = True
    if line.endswith("</text>\n"):
        in_text = False

    if (in_text and line[0] not in "[{}|=# *&!-\n" and "[[" in line and len(line) > 80 and
       random.randint(0, 100) <= 2 and p_test < args.test_size):
        
        test.write(line)
        p_test += 1
        continue

    sys.stdout.write(line)

test.close()
