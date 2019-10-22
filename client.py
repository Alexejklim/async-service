from sys import argv
from argparse import ArgumentParser

from unittest import TestCase
from multiprocessing import Pipe, Process
from socket import socket
from time import sleep

from server import jobs_handler, Service


def tcp_client(message):
    sock = socket()
    sock.connect(('localhost', 8050))

    sock.send(message.encode())
    data = sock.recv(1024)

    sock.close()
    return data.decode('utf-8')


def createParser():
    parser = ArgumentParser()
    parser.add_argument('-m', '--mode', default='simple')
    parser.add_argument('-c', '--command', default='create')
    parser.add_argument('-w', '--work', default='reverse')
    parser.add_argument('-t', '--text')
    parser.add_argument('-i', '--task_id', type=int, default=1)
    return parser


def generate_create_message(namespace):
    if namespace.work in ['reverse', 'transposition'] and namespace.text:
        message = '{"command": "%s", "work": "%s", "text": "%s"}' % ('create', namespace.work, namespace.text)
        return message
    print('Wrong kind of work or text is empty')
    return 'Error'


def generate_state_result_message(namespace, command=None, task_id=None):
    command = check_command(command, namespace)
    task_id = check_task_id(task_id, namespace)

    if command and task_id:
        message = '{"command": "%s", "task_id": %d}' % (command, task_id)
        return message
    print('Wrong command or task_id')
    return 'Error'


def check_command(command, namespace):
    if command:
        return command
    elif namespace.command:
        return namespace.command


def check_task_id(task_id, namespace):
    if task_id:
        return task_id
    elif namespace.task_id:
        return namespace.task_id


def simple_processing(namespace):
    if namespace.command == 'create':
        message = generate_create_message(namespace)
    elif namespace.command in ['state', 'result']:
        message = generate_state_result_message(namespace)
    else:
        print('Wrong command')
        return 'Wrong command'
    if message == 'Error':
        return 'Error'
    print('Result: ', tcp_client(message))
    return 'OK'


def batch_processing(namespace):
    print('Batch processing started')
    message = generate_create_message(namespace)
    if message == 'Error':
        return 'Error'

    task_id = int(tcp_client(message))
    print('Task_ID on server: ', task_id)

    message = generate_state_result_message(namespace, 'state', task_id)
    if message == 'Error':
        return 'Error'

    while True:
        state = tcp_client(message)
        print('Current state: ', state)
        if state == 'done':
            message = generate_state_result_message(namespace, 'result', task_id)
            if message == 'Error':
                return 'Error'

            result = tcp_client(message)
            print('Result: ', result)
            break
        sleep(1)

    return 'OK'


def run(namespace):
    if namespace.mode == 'simple':
        simple_processing(namespace)

    elif namespace.mode == 'batch':
        batch_processing(namespace)
    else:
        print('Wrong program mode')


class namespaces_Test():
    def __init__(self):
        self.mode = ''
        self.command = ''
        self.work = ''
        self.text = ''
        self.task_id = ''


class ServiceTest(TestCase):

    def setUp(self):
        parrent_connection, child_connection = Pipe()
        AsyncServer = Service(parrent_connection)
        self.workman = Process(target=jobs_handler, args=(child_connection,))
        self.workman.start()
        self.service = Process(target=AsyncServer.run)
        self.service.start()
        self.namespace = namespaces_Test()

    def test_simple_processing(self):
        self.assertEqual('Wrong command', simple_processing(self.namespace))
        self.namespace.command = 'create'
        self.assertEqual('Error', simple_processing(self.namespace))
        self.namespace.work = 'reverse'
        self.assertEqual('Error', simple_processing(self.namespace))
        self.namespace.text = 'xyz'
        self.assertEqual('OK', simple_processing(self.namespace))

        self.namespace.command = 'state'
        self.assertEqual('Error', simple_processing(self.namespace))
        self.namespace.task_id = 5
        self.assertEqual('OK', simple_processing(self.namespace))

        self.namespace.command = 'result'
        self.namespace.task_id = 10
        self.assertEqual('OK', simple_processing(self.namespace))

        self.namespace.task_id = 2
        self.assertEqual('OK', simple_processing(self.namespace))

    def test_batch_processing(self):
        self.assertEqual('Error', batch_processing(self.namespace))
        self.namespace.work = 'transposition'
        self.assertEqual('Error', batch_processing(self.namespace))
        self.namespace.text = 'abc'
        self.assertEqual('OK', batch_processing(self.namespace))

    def test_run(self):
        self.assertEqual('2', tcp_client('{"command": "create", "work": "reverse", "text": "abcdefghijklmnop"}'))
        self.assertEqual('3', tcp_client('{"command": "create", "work": "transposition", "text": "abcdefghijklmnop"}'))

        self.assertEqual('not_found', tcp_client('{"command": "state", "task_id": 4}'))
        self.assertEqual('4', tcp_client('{"command": "create", "work": "reverse", "text": "abcdefghijklmnop"}'))
        self.assertEqual('in_queue', tcp_client('{"command": "state", "task_id": 4}'))
        self.assertEqual('not_found', tcp_client('{"command": "result", "task_id": 4}'))
        sleep(3)
        self.assertEqual('in_queue', tcp_client('{"command": "state", "task_id": 4}'))
        self.assertEqual('in_progress', tcp_client('{"command": "state", "task_id":3}'))
        self.assertEqual('done', tcp_client('{"command": "state", "task_id": 2}'))
        sleep(7)
        self.assertEqual('in_progress', tcp_client('{"command": "state", "task_id": 4}'))
        self.assertEqual('done', tcp_client('{"command": "state", "task_id": 3}'))
        self.assertEqual('done', tcp_client('{"command": "state", "task_id": 2}'))
        sleep(1)
        self.assertEqual('not_found', tcp_client('{"command": "result", "task_id": 4}'))
        sleep(2)
        self.assertEqual('ponmlkjihgfedcba', tcp_client('{"command": "result", "task_id": 4}'))
        self.assertEqual('badcfehgjilknmpo', tcp_client('{"command": "result", "task_id": 3}'))
        self.assertEqual('ponmlkjihgfedcba', tcp_client('{"command": "result", "task_id": 2}'))


if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(argv[1:])
    try:
        run(namespace)
    except Exception as ex:
        print('Error: {0}'.format(ex))
