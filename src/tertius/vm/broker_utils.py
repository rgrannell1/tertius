import zmq


def reply(socket: "zmq.Socket[bytes]", requester: bytes, *frames: bytes) -> None:
    socket.send_multipart([requester, *frames])
