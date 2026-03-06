# encoding: utf-8
import asyncio
import logging
from queue import Queue
import time

import grpc
from google.protobuf import json_format
from grpc._channel import _MultiThreadedRendezvous

from . import messages_pb2_grpc
from .messages_pb2 import KaspadMessage

MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024  # 1GB


_logger = logging.getLogger(__name__)

class HtndCommunicationError(Exception): pass


# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto

class HtndThread(object):
    def __init__(self, htnd_host, htnd_port, async_thread=True):

        self.htnd_host = htnd_host
        self.htnd_port = htnd_port

        if async_thread:
            self.channel = grpc.aio.insecure_channel(f'{htnd_host}:{htnd_port}',
                                                     compression=grpc.Compression.Gzip,
                                                     options=[
                                                         ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                                                         ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
                                                     ])
        else:
            self.channel = grpc.insecure_channel(f'{htnd_host}:{htnd_port}',
                                                 compression=grpc.Compression.Gzip,
                                                 options=[
                                                     ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                                                     ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
                                                 ])
            self.__sync_queue = Queue()
        self.stub = messages_pb2_grpc.RPCStub(self.channel)

        self.__queue = asyncio.queues.Queue()

        self.__closing = False

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.__closing = True

    async def request(self, command, params=None, wait_for_response=False, timeout=10):
        if wait_for_response:
            try:
                async for resp in self.stub.MessageStream(self.yield_cmd(command, params), timeout=timeout):
                    self.__queue.put_nowait("done")
                    return json_format.MessageToDict(resp)
            except grpc.aio.AioRpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    import logging
                    logging.error(f"gRPC request timed out (DEADLINE_EXCEEDED) for command {command}. Params: {params}. Returning None.")
                    # Unblock generator to cleanly finish
                    try:
                        self.__queue.put_nowait("done")
                    except Exception:
                        pass
                    return None
                else:
                    raise HtndCommunicationError(str(e))
        else:
            try:
                await self.stub.MessageStream(self.yield_cmd(command, params), timeout=timeout)
                # Unblock generator even when not waiting for response
                try:
                    self.__queue.put_nowait("done")
                except Exception:
                    pass
            except grpc.aio.AioRpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    import logging
                    logging.error(f"gRPC request timed out (DEADLINE_EXCEEDED) for command {command}. Params: {params}. Returning None.")
                    try:
                        self.__queue.put_nowait("done")
                    except Exception:
                        pass
                    return None
                else:
                    raise HtndCommunicationError(str(e))

    async def notify(self, command, params=None, callback_func=None):
        try:
            async for resp in self.stub.MessageStream(self.yield_cmd(command, params)):
                # self.__queue.put_nowait("done")
                if callback_func:
                    await callback_func(json_format.MessageToDict(resp))

        except (grpc.aio._call.AioRpcError, _MultiThreadedRendezvous) as e:
            raise HtndCommunicationError(str(e))

    async def yield_cmd(self, cmd, params=None):
        msg = KaspadMessage()
        msg2 = getattr(msg, cmd)
        payload = params

        if payload:
            if isinstance(payload, dict):
                json_format.ParseDict(payload, msg2)
            if isinstance(payload, str):
                json_format.Parse(payload, msg2)

        msg2.SetInParent()
        yield msg
        await self.__queue.get()

    def yield_cmd_sync(self, cmd, params=None):
        msg = KaspadMessage()
        msg2 = getattr(msg, cmd)
        payload = params

        if payload:
            if isinstance(payload, dict):
                json_format.ParseDict(payload, msg2)
            if isinstance(payload, str):
                json_format.Parse(payload, msg2)

        msg2.SetInParent()
        yield msg
        self.__sync_queue.get()
