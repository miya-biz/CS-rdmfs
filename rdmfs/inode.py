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

class DummyFile:
    def __init__(self, name):
        self.name = name
        self.size = 0

class Inodes:
    def __init__(self, osf, project):
        super(Inodes, self).__init__()
        self.osf = osf
        self.project = project
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self.path_inodes = {}
        self._temp_objects = {}
        self._cache = Cache(maxsize=256, ttl=180, timer=time.time, default=None)

    def exists(self, inode):
        return inode in self.path_inodes

    async def get_osfproject(self):
        if self.osfproject is not None:
            return self.osfproject
        self.osfproject = await self.osf.project(self.project)
        return self.osfproject

    async def _find_file_by_inode(self, inode, allow_dummy):
        if inode not in self.path_inodes:
            return None, None
        path, _ = self.path_inodes[inode]
        return await self._get_file(path, allow_dummy)

    async def _get_file(self, path, allow_dummy=False):
        cached = self._cache_get(path)
        if cached is not None:
            return cached
        temp_object = self._temp_get(path)
        if allow_dummy and temp_object is not None:
            return temp_object
        if len(path) == 1:
            osfproject = await self.get_osfproject()
            storage = await osfproject.storage(path[0])
            self._cache_set(path, (storage, storage))
            return storage, storage
        storage, parent = await self._get_file(path[:-1])
        log.debug('_get_file: path={}, parent={}'.format(path, parent))
        async for file_ in self.get_files(parent):
            if file_.name == path[-1]:
                log.debug('_get_file: name={}'.format(file_.name))
                fileobj = await self._resolve_file(file_)
                self._cache_set(path, (storage, fileobj))
                return storage, fileobj
        log.warning('not found: name={}'.format(path[-1]))
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def _resolve_file(self, file_):
        if hasattr(file_, 'size') and file_.size is not None and type(file_.size) == int:
            return file_
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
        async for f in parent.children:
            yield f

    def register_temp_inode(self, storage, path, name):
        log.debug(f'register_temp_inode: begin path={path}, name={name}')
        path_segments = path.strip('/').split('/')
        if len(path_segments) == 1 and path_segments[0] == '':
            path_segments = []
        newpath = os.path.join(path, name)
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) > 1 and path[0] == storage.name and file_path == newpath:
                log.debug(f'register_temp_inode: end path={path}, inode={inode}')
                return inode
        patho = [storage.name] + path_segments + [name]
        inode = self._register_new_inode(patho, newpath)
        self._temp_set(patho, (storage, DummyFile(name)))
        log.debug(f'register_temp_inode: end path={path}, inode={inode}')
        return inode

    def invalidate_inode(self, storage, target_path):
        target = self._find_inode_by_path(storage, target_path)
        if target is None:
            log.info('Already invalidated: {}, {}'.format(storage.name, target_path))
            return
        path, _ = self.path_inodes[target]
        self._cache_delete(path)
        self._temp_delete(path)
        del self.path_inodes[target]

    def clear_inode_cache(self, storage, target_path):
        target = self._find_inode_by_path(storage, target_path)
        log.debug(f'clear_inode_cache: path={target_path} found={target}')
        if target is None:
            log.info('Not found: {}, {}'.format(storage.name, target_path))
            return
        log.info('Clear cache: inode={}'.format(target))
        path, _ = self.path_inodes[target]
        self._cache_delete(path)
        self._temp_delete(path)

    def _find_inode_by_path(self, storage, target_path):
        for inode, (path, file_path) in self.path_inodes.items():
            if len(path) > 1 and path[0] == storage.name and file_path == target_path:
                return inode
        return None

    def _register_new_inode(self, path, file_path):
        if any([len(p) == 0 for p in path]) > 0:
            raise ValueError('Contains empty filename: {}'.format(path))
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self.path_inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self.path_inodes[new_inode] = (path, file_path)
        self._cache_delete(path)
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

    async def find_by_inode(self, inode, allow_dummy=False):
        log.debug(f'find_by_inode: begin inode={inode}')
        obj = await self._find_by_inode_nocache(inode, allow_dummy)
        log.debug(f'find_by_inode: end inode={inode} obj={obj}')
        return obj

    async def _find_by_inode_nocache(self, inode, allow_dummy):
        if inode == pyfuse3.ROOT_INODE:
            return None, await self.get_osfproject()
        storage, store = await self._find_file_by_inode(inode, allow_dummy)
        if store is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return storage, store

    def _cache_get(self, path):
        return self._cache.get('/'.join(path))

    def _cache_set(self, path, object):
        self._cache.set('/'.join(path), object)

    def _cache_delete(self, path):
        paths = '/'.join(path)
        self._cache.delete(paths)
        self._cache.delete_many(lambda x: x.startswith(paths + '/'))

    def _temp_get(self, path):
        return self._temp_objects.get('/'.join(path), None)

    def _temp_set(self, path, object):
        self._temp_objects['/'.join(path)] = object

    def _temp_delete(self, path):
        if '/'.join(path) not in self._temp_objects:
            return
        del self._temp_objects['/'.join(path)]
