#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

"""
__all__ = ['mustlink']

# Set up the root logger

import logging
import sys

# Set up the root logger
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.INFO)
logformat = format='%(levelname)s %(asctime)s (%(name)s): %(message)s'
stream.setFormatter(logging.Formatter(logformat))

# stream.propagate = False
log.addHandler(stream)

# logging.basicConfig(format='%(levelname)s %(asctime)s (%(name)s): %(message)s',
#                     level=logging.INFO, stream=sys.stdout, datefmt='%Y-%m-%d %H:%M:%S')

