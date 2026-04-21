# Low-level ZMQ socket utilities shared across broker modules.
import zmq


def reply(socket: "zmq.Socket[bytes]", requester: bytes, *frames: bytes) -> None:
    socket.send_multipart([requester, *frames])


def ctrl_send(ctrl: "zmq.Socket[bytes]", *frames: bytes) -> None:
    """Send frames to the broker control socket and discard the ack."""
    ctrl.send_multipart(list(frames))
    ctrl.recv_multipart()
