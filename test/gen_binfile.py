#! /usr/bin/env python
# -*- coding: utf-8 -*-

from struct import *

f = open('./binfile', 'w') 
a='hello'
b='world!'
c=2
d=45.123
e='ÄãºÃ'
#e=e.encode('utf')
bytes=pack('5s6sif4s',a,b,c,d,e)
f.write(bytes)
f.close()

