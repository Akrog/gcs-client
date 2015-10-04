# -*- coding: utf-8 -*-

from functools import wraps


def has_attributes(f, attributes):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        for attribute in attributes:
            if not getattr(self, attribute, None):
                raise Exception('%(func_name)s needs %(attr)s to be set.' %
                                {'func_name': f.__name__, 'attr': attribute})

        return f(self, *args, **kwargs)
    return wrapped
