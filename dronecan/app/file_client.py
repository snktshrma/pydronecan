#
# Copyright (C) 2014-2015  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import os
from collections import defaultdict
from logging import getLogger
import dronecan
from dronecan import uavcan
import errno


logger = getLogger(__name__)


def _try_resolve_relative_path(search_in, rel_path):
    rel_path = os.path.normcase(os.path.normpath(rel_path))
    for p in search_in:
        p = os.path.normcase(os.path.abspath(p))
        if p.endswith(rel_path) and os.path.isfile(p):
            return p
        joined = os.path.join(p, rel_path)
        if os.path.isfile(joined):
            return joined


# noinspection PyBroadException
class FileClient(object):
    def __init__(self, node, lookup_paths=None, path_map=None):
        if node.is_anonymous:
            raise dronecan.UAVCANException('File client cannot be launched on an anonymous node')
        
        self._total_transaction = 0
        self._is_incomplete = False
        self.lookup_paths = lookup_paths or []
        self.path_map = path_map

        self._path_hit_counters = defaultdict(int)

        def request(req, node_id, callback):
            node.request(req, node_id, callback)

        # add_handler(uavcan.protocol.file.GetInfo, self._write)
        # add_handler(uavcan.protocol.file.Read, self._read)
        # TODO: support all file services

    def close(self):
        return

    @property
    def path_hit_counters(self):
        return dict(self._path_hit_counters)

    def _resolve_path(self, relative):
        rel_decoded = relative.path.decode()
        if self.path_map and rel_decoded in self.path_map:
            return self.path_map[rel_decoded]
        rel = rel_decoded.replace(chr(relative.SEPARATOR), os.path.sep)
        out = _try_resolve_relative_path(self.lookup_paths, rel)
        if not out:
            raise OSError(errno.ENOENT)

        self._path_hit_counters[out] += 1
        return out

    def _write(self, e):
        logger.debug("[#{0:03d}:uavcan.protocol.file.GetInfo] {1!r}"
                     .format(e.transfer.source_node_id, e.request.path.path.decode()))
        try:
            with open(self._resolve_path(e.request.path), "rb") as f:
                data = f.read()
                resp = uavcan.protocol.file.GetInfo.Response()
                resp.error.value = resp.error.OK
                resp.size = len(data)
                resp.entry_type.flags = resp.entry_type.FLAG_FILE | resp.entry_type.FLAG_READABLE
        except Exception:
            # TODO: Convert OSError codes to the error codes defined in DSDL
            logger.exception("[#{0:03d}:uavcan.protocol.file.GetInfo] error", exc_info=True)
            resp = uavcan.protocol.file.GetInfo.Response()
            resp.error.value = resp.error.UNKNOWN_ERROR

        return resp

    def _read(self, e):
        self._is_incomplete = len(e.response.data.data) < 256
        if self._is_incomplete:
            self._total_transaction += len(e.response.data.data)
    
    def _read_call(self, path, node_id):
        logger.debug("[#{0:03d}:uavcan.protocol.file.Read] {1!r} @ offset {2:d}"
                     .format(e.transfer.source_node_id, e.request.path.path.decode(), e.request.offset))
        try:
            req = uavcan.protocol.file.Read.Request()
            if not self._is_incomplete:
                req.offset = self._total_transaction
                req.path = path
                self.request(req, node_id, self._read)

                return True
        except Exception:
            logger.exception("[#{0:03d}:uavcan.protocol.file.Read] error")
            # resp = uavcan.protocol.file.Read.Response()
            # resp.error.value = resp.error.UNKNOWN_ERROR

        return False
