#!/usr/bin/python

with open('README.md', 'w') as f:
    f.write(open('devtools/README.template.md', 'r').read())

    for line in open('examples/twitty.py', 'r').readlines():
        f.write('    ' + line)