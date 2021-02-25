import re
import json
import pprint
import sys

f = open(sys.argv[1])
lines = f.readlines()
sondetypes = {}

for l in lines:
    l = l.rstrip('\n')
    if l.startswith('#'):
        continue
    #print(l)
    id, text = l.split('\t', 1)

    m = re.search('\d+-\d+', id)
    if m:
        l,u = id.split('-')
        for i in range(int(l),int(u)+1):
            sondetypes[i] = text
    else:
        sondetypes[int(id)] = text

with open(sys.argv[1]+'.json', "wb") as jfile:
    #j = json.dumps(sondetypes, indent=4).encode("utf8")
    j = pprint.pformat(sondetypes).encode("utf8")
    jfile.write(j)
