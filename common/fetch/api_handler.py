import os
import json
import requests
from datetime import datetime
import subprocess
import time

class ApiHandler:
    def __init__(self, config):
        self.config = config
