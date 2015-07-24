# -*-coding:utf8-*-
import os
import shutil
import subprocess
import time

from threading import Thread
from boto.dynamodb2.layer1 import DynamoDBConnection
from bynamodb.model import Model
from bynamodb.patcher import patch_dynamodb_connection
from twisted.internet import reactor

import config
from dnachat.models import Message, Channel, ChannelJoinInfo, ChannelUsageLog, ChannelWithdrawalLog
from dnachat.runner import run_dnachat, run_logger
from dnachat import models


class ChatServerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True

    def run(self):
        run_dnachat('tests/config.py')


class ChatLoggerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True

    def run(self):
        run_logger('tests/config.py')

chat_server = None
db_server = None


def pytest_configure():
    global chat_server
    global db_server

    chat_server = ChatServerThread()
    chat_server.start()

    ChatLoggerThread().start()

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
    os.putenv("PYTEST_TIMEOUT", "3")


def pytest_runtest_teardown():
    conn = DynamoDBConnection()
    conn.delete_table(Message.get_table_name())
    conn.delete_table(ChannelJoinInfo.get_table_name())
    conn.delete_table(ChannelUsageLog.get_table_name())
    conn.delete_table(Channel.get_table_name())
    conn.delete_table(ChannelWithdrawalLog.get_table_name())

    Message.create_table()
    ChannelWithdrawalLog.create_table()
    ChannelJoinInfo.create_table()
    ChannelUsageLog.create_table()
    Channel.create_table()


def pytest_unconfigure():
    shutil.rmtree('tests/local_dynamodb/testdb', True)
    db_server.terminate()
    reactor.stop()
