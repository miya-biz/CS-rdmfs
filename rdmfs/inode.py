from datetime import datetime
import json
import os
import sys
import logging
import errno
import time
import pyfuse3
import pyfuse3_asyncio
from cacheout import Cache
from . import node
from osfclient.models.file import File

log = logging.getLogger(__name__)

def fromisoformat(datestr):
    datestr = datestr.replace('Z', '+00:00')
    return int(datetime.fromisoformat(datestr).timestamp() * 1e9)

class Inodes:
    def __init__(self, osf, project):
        super(Inodes, self).__init__()
        self.osf = osf
        self.project = project
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self.path_inodes = {}
        self.cache = Cache(maxsize=256, ttl=180, timer=time.time, default=None)

    def exists(self, inode):
        return inode in self.path_inodes

    async def get_osfproject(self):
        if self.osfproject is not None:
            return self.osfproject
        self.osfproject = await self.osf.project(self.project)
        return self.osfproject

    async def _find_storage_by_inode(self, inode):
        if inode not in self.path_inodes:
            return None
        path, _ = self.path_inodes[inode]
        if len(path) > 1:
            return None
        osfproject = await self.get_osfproject()
        return await osfproject.storage(path[0])

    async def _find_file_by_inode(self, inode):
        if inode not in self.path_inodes:
            return None, None
        path, _ = self.path_inodes[inode]
        return await self._get_file(path)

    async def _get_file(self, path):
        if len(path) == 1:
            osfproject = await self.get_osfproject()
            storage = await osfproject.storage(path[0])
            return storage, storage
        storage, parent = await self._get_file(path[:-1])
        log.info('_get_file: path={}, parent={}'.format(path, parent))
        async for file_ in self.get_files(parent):
            if file_.name == path[-1]:
                log.info('_get_file: name={}'.format(file_.name))
                return storage, await self._resolve_file(file_)
        log.error('not found: name={}'.format(path[-1]))
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def _resolve_file(self, file_):
        if not hasattr(file_, '_upload_url'):
            return file_
        url = file_._upload_url + '?meta='
        log.info('_resolve_file: url={}'.format(url))
        response = file_._json(await file_._get(url), 200)
        log.info('_resolve_file: json={}'.format(json.dumps(response)))
        data = response['data']
        data['links']['self'] = None
        data['attributes']['materialized_path'] = data['attributes']['materialized']
        data['attributes']['date_created'] = data['attributes']['created_utc']
        data['attributes']['date_modified'] = data['attributes']['modified_utc']
        return File(data, file_.session)

    async def get_files(self, parent):
        if hasattr(parent, 'child_files'):
            async for f in parent.child_files:
                yield f
            async for f in parent.child_folders:
                yield f
        else:
            async for f in parent.files:
                yield f
            async for f in parent.folders:
                yield f

    def _register_new_inode(self, path, file_path):
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self.path_inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self.path_inodes[new_inode] = (path, file_path)
        return new_inode

    def get_storage_inode(self, storage):
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) == 1 and path[0] == storage.name:
                return inode
        return self._register_new_inode([storage.name], None)

    def get_file_inode(self, storage, file_):
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) > 1 and path[0] == storage.name and file_path == file_.path:
                return inode
        log.info('_get_file_inode, path={}'.format(file_.path))
        path_segments = file_.path.strip('/').split('/')
        return self._register_new_inode([storage.name] + path_segments, file_.path)

    async def find_by_inode(self, inode):
        cached = self.cache.get(inode)
        if cached is not None:
            return cached
        obj = await self._find_by_inode_nocache(inode)
        self.cache.set(inode, obj)
        return obj

    async def _find_by_inode_nocache(self, inode):
        if inode == pyfuse3.ROOT_INODE:
            return None, await self.get_osfproject()
        store = await self._find_storage_by_inode(inode)
        if store is not None:
            return store, store
        storage, store = await self._find_file_by_inode(inode)
        if store is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return storage, store
