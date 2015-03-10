# -*-coding:utf8-*-
import os
import pytest
import shutil
import subprocess
import time

from threading import Thread
from bynamodb.model import Model
from bynamodb.patcher import patch_dynamodb_connection
from twisted.internet import reactor
from twisted.internet import task

import config
from dnachat.runner import run_dnachat
from dnachat import models


class ChatServerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._stop = False

    def run(self):
        lc = task.LoopingCall(self.check_stop_flag)
        lc.start(2)
        run_dnachat('tests/config.py')

    def check_stop_flag(self):
        if self._stop:
            reactor.stop()

    def terminate(self):
        self._stop = True


chat_server = None
db_server = None


def pytest_configure():
    global chat_server
    global db_server

    chat_server = ChatServerThread()
    chat_server.start()

    shutil.rmtree('tests/local_dynamodb/testdb', True)
    os.mkdir('tests/local_dynamodb/testdb')

    dev_null = open(os.devnull, 'wb')
    db_server = subprocess.Popen([
        '/usr/bin/java', '-Djava.net.preferIPv4Stack=true',
        '-Djava.library.path=tests/local_dynamodb/DynamoDBLocal_lib', '-jar',
        'tests/local_dynamodb/DynamoDBLocal.jar', '-port', str(config.DYNAMODB_CONNECTION['port']),
        '-dbPath', 'tests/local_dynamodb/testdb/'
    ], stdout=dev_null, stderr=dev_null)
    patch_dynamodb_connection(**config.DYNAMODB_CONNECTION)

    for name in dir(models):
        obj = getattr(models, name)
        if isinstance(obj, type) and issubclass(obj, Model) and obj is not Model:
            obj.create_table()

    time.sleep(0.5)


def pytest_unconfigure():
    shutil.rmtree('tests/local_dynamodb/testdb', True)
    db_server.terminate()
    chat_server.terminate()
