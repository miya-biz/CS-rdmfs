from datetime import datetime
import json
import sys
import logging
import errno
import time
from typing import Optional, Union, List, Dict, AsyncGenerator

import pyfuse3
from cacheout import Cache
from osfclient import OSF
from osfclient.models import Storage, Project, File, Folder


log = logging.getLogger(__name__)
FILE_ATTRIBUTE_CACHE_TTL = 60 # 1 minute
LIST_CACHE_TTL = 180 # 3 minutes


def fromisoformat(datestr):
    datestr = datestr.replace('Z', '+00:00')
    return int(datetime.fromisoformat(datestr).timestamp() * 1e9)


class DummyFile:
    def __init__(self, name):
        self.name = name
        self.size = 0


class BaseInode:
    """The class for managing single inode."""
    def __init__(self, id: int):
        if not isinstance(id, int):
            raise ValueError('Invalid inode id: {}'.format(id))
        self.id = id

    def __str__(self) -> str:
        return f'<{self.__class__.__name__} [id={self.id}, path={self.path}]>'

    async def refresh(self, context: 'Inodes', force=False):
        log.debug(f'nothing to refresh: inode={self.id}')

    def invalidate(self):
        pass

    @property
    def parent(self) -> Optional['BaseInode']:
        raise NotImplementedError

    @property
    def storage(self) -> 'StorageInode':
        raise NotImplementedError

    @property
    def object(self):
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def size(self) -> Optional[Union[int, str]]:
        return None

    def has_children(self) -> bool:
        return False

    @property
    def date_created(self) -> Optional[str]:
        return None

    @property
    def date_modified(self) -> Optional[str]:
        return None

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def display_path(self) -> str:
        raise self.path


class ProjectInode(BaseInode):
    """The class for managing single project inode."""
    def __init__(self, id: int, project: Project):
        super(ProjectInode, self).__init__(id)
        self.project = project

    @property
    def parent(self) -> Optional[BaseInode]:
        return None

    @property
    def storage(self):
        raise ValueError('Project inode does not have storage')

    @property
    def object(self):
        return self.project

    @property
    def name(self):
        return self.project.title

    def has_children(self):
        return True

    @property
    def path(self):
        return f'/{self.project.id}/'

    @property
    def display_path(self):
        return '/'


class StorageInode(BaseInode):
    """The class for managing single storage inode."""
    def __init__(self, id: int, project: ProjectInode, storage: Storage):
        super(StorageInode, self).__init__(id)
        self.project = project
        self._storage = storage

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project

    @property
    def storage(self):
        return self

    @property
    def object(self):
        return self._storage

    @property
    def name(self):
        return self._storage.name

    def has_children(self):
        return True

    @property
    def path(self):
        return f'{self.parent.path}{self._storage.name}/'

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self._storage.name}/'


class FolderInode(BaseInode):
    """The class for managing single folder inode."""
    def __init__(self, id: int, parent: BaseInode, folder: Folder):
        super(FolderInode, self).__init__(id)
        self._parent = parent
        self.folder = folder

    @property
    def parent(self) -> Optional[BaseInode]:
        return self._parent

    @property
    def storage(self):
        return self._parent.storage

    @property
    def object(self):
        return self.folder

    @property
    def name(self):
        return self.folder.name

    def has_children(self):
        return True

    @property
    def date_created(self) -> Optional[str]:
        return self.folder.date_created

    @property
    def date_modified(self) -> Optional[str]:
        return self.folder.date_modified

    @property
    def _path(self):
        try:
            return self.folder.osf_path
        except AttributeError:
            return self.folder.path

    @property
    def path(self):
        path = self._path if not self._path.startswith('/') else self._path[1:]
        return f'{self.storage.path}{path}'

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}/'


class NewFile:
    """Dummy class for new file."""
    def __init__(self, parent: BaseInode, name: str):
        self.parent = parent
        self.name = name

    @property
    def path(self):
        return f'{self.parent.path}{self.name}'


class FileInode(BaseInode):
    """The class for managing single file inode."""
    _updated: Optional[File]
    _last_loaded: float

    def __init__(
        self,
        id: int,
        parent: BaseInode,
        file: Union[File, NewFile],
    ):
        super(FileInode, self).__init__(id)
        self._parent = parent
        self.file = file
        self._updated = None
        self._last_loaded = time.time()

    def invalidate(self):
        self._last_loaded = None

    async def refresh(self, context: 'Inodes', force=False):
        expired = self._last_loaded is None or \
            time.time() - self._last_loaded > FILE_ATTRIBUTE_CACHE_TTL
        if not force and not isinstance(self.file, NewFile) and not expired:
            return
        child = await self._get_child_by_name(self.file.name)
        if child is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if isinstance(child, Folder):
            raise pyfuse3.FUSEError(errno.EISDIR)
        self._updated = child
        self._last_loaded = time.time()
        if isinstance(self.file, NewFile):
            self.file = self._updated

    @property
    def parent(self) -> Optional[BaseInode]:
        return self._parent

    @property
    def storage(self):
        return self._parent.storage

    @property
    def object(self):
        return self.file

    @property
    def name(self):
        return self.file.name

    @property
    def _latest(self):
        return self._updated or self.file

    @property
    def size(self) -> Optional[Union[int, str]]:
        if not hasattr(self._latest, 'size'):
            return None
        if self._latest.size is None:
            return None
        if type(self._latest.size) != int and type(self._latest.size) != str:
            return None
        return self._latest.size

    @property
    def date_created(self) -> Optional[str]:
        return self._latest.date_created

    @property
    def date_modified(self) -> Optional[str]:
        return self._latest.date_modified

    @property
    def _path(self):
        try:
            return self.file.osf_path
        except AttributeError:
            return self.file.path

    @property
    def path(self):
        path = self._path if not self._path.startswith('/') else self._path[1:]
        return f'{self.storage.path}{path}'

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}'

    async def _get_child_by_name(self, name: str) -> Optional[Union[File, Folder]]:
        async for child in self._parent.object.children:
            if child.name == name:
                return child
        return None


class ChildRelation:
    """The class for managing child relations."""
    def __init__(self, parent: BaseInode, children: List[BaseInode]):
        """Initialize ChildRelation object."""
        self.parent = parent
        self.children = children


class Inodes:
    """The class for managing multiple inodes."""
    INODE_DUMMY = -1
    osf: OSF
    project: str
    osfproject: Optional[Project]
    _inodes: Dict[int, BaseInode]
    _child_relations: Cache

    def __init__(self, osf: OSF, project: str):
        """Initialize Inodes object."""
        super(Inodes, self).__init__()
        self.osf = osf
        self.project = project
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self._inodes = {}
        self._child_relations = Cache(maxsize=256, ttl=LIST_CACHE_TTL, timer=time.time, default=None)

    async def _get_osfproject(self):
        """Get OSF project object."""
        if self.osfproject is not None:
            return self.osfproject
        self.osfproject = await self.osf.project(self.project)
        return self.osfproject

    def register(self, parent_inode: BaseInode, name: str):
        """Register new inode."""
        log.debug(f'register: path={parent_inode}, name={name}')
        newfile = NewFile(parent_inode, name)
        return self._get_object_inode(parent_inode, newfile)

    def invalidate(self, inode: Union[int, BaseInode]):
        """Invalidate inode.

        If inode is integer, it is treated as inode number."""
        log.debug(f'invalidate: inode={inode}')
        if isinstance(inode, int):
            if inode not in self._inodes:
                raise ValueError('Unexpected inode: {}'.format(inode))
            inode = self._inodes[inode]
        self._child_relations.delete(inode.id)
        inode.invalidate()

    async def get(self, inode_num: int) -> Optional[BaseInode]:
        """Find inode by inode number."""
        if inode_num in self._inodes:
            return self._inodes[inode_num]
        if inode_num == pyfuse3.ROOT_INODE:
            project = await self._get_osfproject()
            inode = ProjectInode(pyfuse3.ROOT_INODE, project)
            self._inodes[inode_num] = inode
            return inode
        return None

    async def find_by_name(self, parent: BaseInode, name: str) -> Optional[BaseInode]:
        """Find inode by name."""
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        cache: Optional[ChildRelation] = self._child_relations.get(parent.id)
        if cache is not None:
            # Use cache
            for child in cache.children:
                if child.name == name:
                    return child
        # Request to GRDM
        found = None
        children: List[BaseInode] = []
        async for child in self.get_children_of(parent):
            children.append(self._get_object_inode(parent, child))
            if child.name == name:
                found = child
        cache = ChildRelation(parent, children)
        self._child_relations.set(parent.id, cache)
        if found is None:
            return None
        return self._get_object_inode(parent, found)

    async def get_children_of(self, parent: BaseInode) -> AsyncGenerator[Union[Storage, File, Folder], None]:
        """Get children of the parent inode."""
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        if isinstance(parent, ProjectInode):
            project = parent.object
            async for storage in project.storages:
                yield storage
            return
        async for child in parent.object.children:
            yield child

    def _get_object_inode(self, parent: BaseInode, object: Union[Storage, File, Folder]) -> BaseInode:
        """Get inode for the object."""
        dummy_inode = self._create_object_inode(self.INODE_DUMMY, parent, object)
        for inode in self._inodes.values():
            if inode.path == dummy_inode.path:
                return inode
        # Register new inode
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self._inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self._inodes[new_inode] = r = self._create_object_inode(new_inode, parent, object)
        log.debug(f'new inode: inode={r}')
        return r

    def _create_object_inode(self, inode_num: int, parent: BaseInode, object: Union[Storage, File, Folder]) -> BaseInode:
        """Create inode object for the object."""
        if isinstance(object, Storage):
            return StorageInode(inode_num, parent, object)
        if isinstance(object, Folder):
            return FolderInode(inode_num, parent, object)
        return FileInode(inode_num, parent, object)
