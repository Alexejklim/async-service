# asynchrony on generators with round robin
# https://www.youtube.com/watch?v=ys8lW8eQaJQ

# protocol
# commands: create, state, result
# type of works: reverse, transposition

import socket
from json import loads
from logging import info, basicConfig, DEBUG
from select import select
from time import time, sleep
from multiprocessing import Pipe, Process
from threading import Thread


class Service():
    def __init__(self, connection):
        self.parrent_connection = connection
        self.to_read = {}
        self.to_write = {}
        self.workers = []
        self.task_states = {}
        self.task_results = {}
        self.task_index = 0

    def startserver(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('localhost', 8050))
        self.server_socket.listen()
        info('Waiting for connect')

        while True:
            yield ('read_socket', self.server_socket)
            client_socket, addr = self.server_socket.accept()
            info('Client connected, {0}'.format(addr))
            self.workers.append(self.client_handler(client_socket))

    def client_handler(self, client_socket):
        while True:
            yield ('write_socket', client_socket)
            info('Read message')
            request = client_socket.recv(1024)

            if not request:
                break
            else:
                response = self.request_handler(request)
                client_socket.send(response)
                info('Send message, {0}'.format(response))
            client_socket.close()

    def request_handler(self, request):
        info('trying to parse request')
        try:
            message = loads(request.decode('utf-8'))
            command = message.get('command')
            answer = ''

            if command == 'create':
                work = message.get('work')
                text = message.get('text')
                self.task_index += 1
                self.task_states[self.task_index] = 'in_queue'
                self.parrent_connection.send((self.task_index, work, text))
                answer = str(self.task_index)
            elif command == 'state':
                task_id = int(message.get('task_id'))
                answer = self.task_states.get(task_id)
            elif command == 'result':
                task_id = int(message.get('task_id'))
                if self.task_states.get(task_id) == 'done':
                    answer = self.task_results.get(task_id)
        except Exception as ex:
            info('Parse error, {0}, {1}'.format(ex, request))

        if not answer:
            answer = 'not_found'

        return answer.encode()

    def read_pipe(self):
        while True:
            state, ind, result = self.parrent_connection.recv()
            info('read message from pipe')
            if state == 'state':
                self.task_states[ind] = result
            elif state == 'result':
                self.task_results[ind] = result

    def run(self):
        self.workers.append(self.startserver())
        pipe_reader = Thread(target=self.read_pipe)
        pipe_reader.start()
        # round robin
        while any([self.workers, self.to_read, self.to_write]):
            while not self.workers:
                ready_to_read, ready_to_write, _ = select(self.to_read, self.to_write, [])
                for sock in ready_to_read:
                    self.workers.append(self.to_read.pop(sock))

                for sock in ready_to_write:
                    self.workers.append(self.to_write.pop(sock))
            try:
                task = self.workers.pop(0)
                reason, sock = next(task)

                if reason == 'read_socket':
                    self.to_read[sock] = task
                if reason == 'write_socket' and sock.fileno() != -1:
                    self.to_write[sock] = task
            except Exception as ex:
                info('loop_error, {0}'.format(ex))


def jobs_handler(child_connection):
    basicConfig(filename="worker.log", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=DEBUG)
    while True:
        try:
            info('waiting for job from pipe')
            ind, work, text = child_connection.recv()
            child_connection.send(('state', ind, 'in_progress'))

            if work == 'reverse':
                result = reverse(text)
                child_connection.send(('state', ind, 'done'))
                child_connection.send(('result', ind, result))
            elif work == 'transposition':
                result = transposition(text)
                child_connection.send(('state', ind, 'done'))
                child_connection.send(('result', ind, result))
            else:
                child_connection.send(('state', ind, 'error'))
        except Exception as ex:
            info('handle_error: {0}'.format(ex))


def reverse(text):
    info('reverse text {0}'.format(text))
    timestart = time()
    result = text[::-1]
    sleep(3 - (time() - timestart))
    return result


def transposition(text):
    info('transposition text {0}'.format(text))
    timestart = time()
    result = ''

    for x, y in zip(text[1::2], text[::2]):
        result += x + y
    if len(text) % 2 != 0:
        result += text[len(text) - 1]
    sleep(7 - (time() - timestart))
    return result


if __name__ == '__main__':
    basicConfig(filename="service.log", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=DEBUG)
    parrent_connection, child_connection = Pipe()
    AsyncServer = Service(parrent_connection)
    workman = Process(target=jobs_handler, args=(child_connection,))
    workman.start()
    AsyncServer.run()
    workman.join()
