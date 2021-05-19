from datetime import datetime
import json
import os
import sys
import stat
import logging
import errno
import time
import traceback
import pyfuse3
import pyfuse3_asyncio
from osfclient import exceptions as osf_exceptions
from . import node
from .inode import Inodes, fromisoformat
from .filehandle import FileHandlers

log = logging.getLogger(__name__)

class RDMFileSystem(pyfuse3.Operations):
    def __init__(self, osf, project, dir_mode=0o755, file_mode=0o644, uid=None, gid=None):
        super(RDMFileSystem, self).__init__()
        self.inodes = Inodes(osf, project)
        self.file_handlers = FileHandlers()
        self.dir_mode = dir_mode
        self.file_mode = file_mode
        self.uid = uid or os.getuid()
        self.gid = gid or os.getgid()

    async def getattr(self, inode, ctx=None):
        try:
            log.info('getattr: inode={inode}'.format(inode=inode))
            entry = pyfuse3.EntryAttributes()
            storage, store = await self.inodes.find_by_inode(inode)
            if hasattr(store, 'files') or hasattr(store, 'storages'):
                entry.st_mode = (stat.S_IFDIR | self.dir_mode)
                entry.st_size = 0
            else:
                entry.st_mode = (stat.S_IFREG | self.file_mode)
                log.info('getattr: name={}, size={}'.format(store.name, store.size))
                if store.size is not None:
                    entry.st_size = int(store.size)
                else:
                    entry.st_size = 0
            stamp = 0
            mstamp = stamp
            if hasattr(store, 'date_created') and store.date_created is not None:
                stamp = fromisoformat(store.date_created)
            if hasattr(store, 'date_modified') and store.date_modified is not None:
                mstamp = fromisoformat(store.date_modified)
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = stamp
            entry.st_gid = self.gid
            entry.st_uid = self.uid
            entry.st_ino = inode
            return entry
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def setattr(self, inode, attr, fields, fh, ctx=None):
        try:
            log.info('setattr: inode={inode}, attr={attr}, fh={fh}'.format(
                inode=inode, attr=attr, fh=fh
            ))
            log.info('not supported')
            return await self.getattr(inode)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def lookup(self, parent_inode, bname, ctx=None):
        try:
            name = bname.decode('utf8')
            log.info('lookup parent_inode={parent_inode}, name={name}'.format(
                parent_inode=parent_inode, name=name
            ))
            if parent_inode == pyfuse3.ROOT_INODE:
                # Storages
                osfproject = await self.inodes.get_osfproject()
                storage = None
                async for s in osfproject.storages:
                    if s.name == name:
                        storage = s
                if storage is None:
                    raise pyfuse3.FUSEError(errno.ENOENT)
                inode = self.inodes.get_storage_inode(storage)
                return await self.getattr(inode)
            # Files
            storage, store = await self.inodes.find_by_inode(parent_inode)
            if store is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if not hasattr(store, 'files'):
                raise pyfuse3.FUSEError(errno.ENOENT)
            target = None
            async for file_ in self.inodes.get_files(store):
                if file_.name == name:
                    target = file_
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            inode = self.inodes.get_file_inode(storage, target)
            return await self.getattr(inode)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def opendir(self, inode, ctx):
        log.info('opendir: inode={inode}'.format(inode=inode))
        try:
            if inode == pyfuse3.ROOT_INODE:
                osfproject = await self.inodes.get_osfproject()
                return self.file_handlers.get_node_fh(node.Project(self, osfproject))
            if self.inodes.exists(inode):
                storage, store = await self.inodes.find_by_inode(inode)
                log.info('find_by_inode: storage={}, folder={}'.format(storage, store))
                return self.file_handlers.get_node_fh(node.Folder(self, storage, store))
            raise pyfuse3.FUSEError(errno.ENOENT)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def readdir(self, fh, start_id, token):
        log.info('readdir: fh={fh}, start_id={start_id}'.format(
            fh=fh, start_id=start_id
        ))
        try:
            folder = self.file_handlers.find_node_by_fh(fh)
            assert folder is not None
            await folder.readdir(start_id, token)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)
        return

    async def setxattr(self, inode, name, value, ctx):
        log.info('setxattr')
        if inode != pyfuse3.ROOT_INODE or name != b'command':
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        if value == b'terminate':
            pyfuse3.terminate()
        else:
            raise pyfuse3.FUSEError(errno.EINVAL)

    async def open(self, inode, flags, ctx):
        try:
            log.info('open: inode={inode}, flags={flags}'.format(inode=inode, flags=flags))
            if not self.inodes.exists(inode):
                raise pyfuse3.FUSEError(errno.ENOENT)
            storage, store = await self.inodes.find_by_inode(inode)
            return pyfuse3.FileInfo(fh=self.file_handlers.get_node_fh(
                node.File(self, storage, store, flags)
            ))
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def create(self, parent_inode, name, mode, flags, ctx):
        try:
            sname = name.decode('utf8')
            log.info('create: parent_inode={inode} name={sname}'.format(
                inode=parent_inode, sname=sname
            ))
            storage, store = await self.inodes.find_by_inode(parent_inode)
            log.info('create: parent_path={}'.format(store.path))
            newpath = os.path.join(store.path.lstrip('/'), sname)

            entry = pyfuse3.EntryAttributes()
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_size = 0
            stamp = int(datetime.now().timestamp() * 1e9)
            mstamp = stamp
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = stamp
            entry.st_gid = os.getgid()
            entry.st_uid = os.getuid()
            entry.st_ino = self.inodes.register_temp_inode(storage, store.path, sname)

            return (
                pyfuse3.FileInfo(fh=self.file_handlers.get_node_fh(
                    node.NewFile(self, storage, newpath, flags)
                )),
                entry
            )
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def read(self, fh, off, size):
        try:
            log.info('read: fh={fh}, off={off}, size={size}'.format(
                fh=fh, off=off, size=size
            ))
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            return await file_.read(off, size)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def write(self, fh, off, buf):
        try:
            log.info('write: fh={fh}, off={off}'.format(fh=fh, off=off))
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            return await file_.write(off, buf)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def flush(self, fh):
        try:
            log.info('flush: fh={fh}'.format(fh=fh))
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.flush()
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def release(self, fh):
        try:
            log.info('release')
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.close()
            self.file_handlers.release_fh(fh)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def releasedir(self, fh):
        try:
            log.info('releasedir')
            file_ = self.file_handlers.find_node_by_fh(fh)
            assert file_ is not None
            await file_.close()
            self.file_handlers.release_fh(fh)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def mkdir(self, parent_inode, name, mode, ctx):
        try:
            storage, store = await self.inodes.find_by_inode(parent_inode)
            if storage is None:
                # root inode
                raise pyfuse3.FUSEError(errno.ENOSYS)
            new_folder = await store.create_folder(name.decode('utf8'))
            new_attr = await self.lookup(parent_inode, name)
            log.info('mkdir: folder={}, attr={}'.format(new_folder, new_attr))
            return new_attr
        except osf_exceptions.FolderExistsException:
            raise pyfuse3.FUSEError(errno.EEXIST)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def rmdir(self, parent_inode, name, ctx):
        try:
            storage, store = await self.inodes.find_by_inode(parent_inode)
            if storage is None:
                # root inode
                raise pyfuse3.FUSEError(errno.ENOSYS)
            target = None
            sname = name.decode('utf8')
            async for file_ in self.inodes.get_files(store):
                if file_.name == sname:
                    target = file_
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if not hasattr(target, 'files'):
                log.info('attempt to rmdir file: {}'.format(target))
                raise pyfuse3.FUSEError(errno.ENOTDIR)
            async for _ in self.inodes.get_files(target):
                raise pyfuse3.FUSEError(errno.ENOTEMPTY)
            log.info('rmdir: folder={}'.format(target))
            await target.remove()
            self.inodes.invalidate_inode(storage, target.path)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        try:
            storage_old, store_old = await self.inodes.find_by_inode(parent_inode_old)
            if storage_old is None:
                # root inode
                raise pyfuse3.FUSEError(errno.ENOSYS)
            storage_new, store_new = await self.inodes.find_by_inode(parent_inode_new)
            if storage_new is None:
                # root inode
                raise pyfuse3.FUSEError(errno.ENOSYS)
            target_old = None
            sname_old = name_old.decode('utf8')
            async for file_ in self.inodes.get_files(store_old):
                if file_.name == sname_old:
                    target_old = file_
            if target_old is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            sname_new = name_new.decode('utf8')
            if sname_old != sname_new:
                if hasattr(target_old, 'files'):
                    await target_old.move_to(storage_new, store_new, to_foldername=sname_new)
                else:
                    await target_old.move_to(storage_new, store_new, to_filename=sname_new)
            else:
                await target_old.move_to(storage_new, store_new)
            log.info('move: src={}, dest={}, destname={}'.format(
                target_old, store_new, sname_new
            ))
            self.inodes.invalidate_inode(storage_old, target_old.path)
            self.inodes.invalidate_inode(storage_new, os.path.join(store_new.path, sname_new))
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)

    async def unlink(self, parent_inode, name, ctx):
        try:
            storage, store = await self.inodes.find_by_inode(parent_inode)
            if storage is None:
                # root inode
                raise pyfuse3.FUSEError(errno.ENOSYS)
            target = None
            sname = name.decode('utf8')
            async for file_ in self.inodes.get_files(store):
                if file_.name == sname:
                    target = file_
            if target is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            log.info('unlink: file={}'.format(target))
            await target.remove()
            self.inodes.invalidate_inode(storage, target.path)
        except pyfuse3.FUSEError as e:
            raise e
        except:
            traceback.print_exc()
            raise pyfuse3.FUSEError(errno.EBADF)
