#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .dynamodbagency import DynamoDBAgency


class DynamoDBAgent(DynamoDBAgency):
    def __init__(self, logger, **setting):
        DynamoDBAgency.__init__(self, logger, **setting)
