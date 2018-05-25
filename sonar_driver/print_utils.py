"""
Printing Utilities
"""

from __future__ import print_function
import json

from pygments import highlight, lexers, formatters


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def pretty_print(title, data, lexer=lexers.JsonLexer()):
    """ Format and print pretty output using pygments """

    if lexer.name == 'JSON':
        data = json.dumps(data, sort_keys=True, indent=4)

    colorful_json = highlight(data, lexer, formatters.TerminalFormatter())
    print(title + ':')
    print(colorful_json)

