"""
    refcounting
    ~~~~~~~~~~~

    Reference count annotations for C API functions. Has the same
    result as the sphinx.ext.refcounting extension but works for all
    functions regardless of the signature, and the reference counting
    information is written inline with the documentation instead of a
    separate file.

    Adds a new directive "refcounting". The directive has no content
    and one required positional parameter:: "new" or "borrow".

    Example:

    .. cfunction:: json_t *json_object(void)

       .. refcounting:: new

       <description of the json_object function>

    :copyright: Copyright (c) 2009-2016 Petri Lehtinen <petri@digip.org>
    :license: MIT, see LICENSE for details.
"""

from docutils import nodes
from docutils.parsers.rst import Directive


def visit(self, node):
    self.visit_emphasis(node)

def depart(self, node):
    self.depart_emphasis(node)

def html_visit(self, node):
    self.body.append(self.starttag(node, 'em', '', CLASS='refcount'))

def html_depart(self, node):
    self.body.append('</em>')


class refcounting(nodes.emphasis):
    pass

class refcounting_directive(Directive):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    def run(self):
        if self.arguments[0] == 'borrow':
            text = 'Return value: Borrowed reference.'
        elif self.arguments[0] == 'new':
            text = 'Return value: New reference.'
        else:
            raise Error('Valid arguments: new, borrow')

        return [refcounting(text, text)]


def setup(app):
    app.add_node(refcounting,
                 html=(html_visit, html_depart),
                 latex=(visit, depart),
                 text=(visit, depart),
                 man=(visit, depart))
    app.add_directive('refcounting', refcounting_directive)
