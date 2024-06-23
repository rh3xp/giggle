#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
from pymongo import ASCENDING, DESCENDING


ROOT_PATH = os.path.abspath(__file__).rsplit("/", 2)[0]
sys.path.append(ROOT_PATH)

# Connection string variables
CONNECTION_INSTANCE = 'local'
HOSTNAME = 'localhost'
PORT = 27017
USERNAME = ""
PASSWORD = ""



# Databse and Collection details
DB_NAME = "giggles"
COLLECTION_NAME = "links"
FETCH_LIMIT = 5
SORT_ORDER = ASCENDING
print(type(SORT_ORDER))
