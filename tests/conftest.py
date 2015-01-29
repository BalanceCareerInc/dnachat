# -*-coding:utf8-*-
from threading import Thread
from dnachat.runner import run_dnachat


class ChatServerThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        run_dnachat('tests/test_config.py')


chat_server = None


def pytest_configure():
    global chat_server
    chat_server = ChatServerThread()
    chat_server.start()


def pytest_unconfigure():
    chat_server.stop()
