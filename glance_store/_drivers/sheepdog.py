# Copyright 2013 Taobao Inc.
# Copyright (C) 2015 Nippon Telegraph and Telephone Corporation.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Storage backend for Sheepdog storage system"""

import hashlib
import logging

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_utils import excutils
from oslo_utils import units

import glance_store
from glance_store import capabilities
from glance_store.common import utils
import glance_store.driver
from glance_store import exceptions
from glance_store.i18n import _, _LE
import glance_store.location

# Sheepdog VDI snapshot name for glance image
GLANCE_SNAPNAME = 'glance-image'

LOG = logging.getLogger(__name__)

DEFAULT_ADDR = '127.0.0.1'
DEFAULT_PORT = 7000
DEFAULT_CHUNKSIZE = 64  # in MiB

_SHEEPDOG_OPTS = [
    cfg.IntOpt('sheepdog_store_chunk_size', default=DEFAULT_CHUNKSIZE,
               help=_('Images will be chunked into objects of this size '
                      '(in megabytes). For best performance, this should be '
                      'a power of two.')),
    cfg.IntOpt('sheepdog_store_port', default=DEFAULT_PORT,
               help=_('Port of sheep daemon.')),
    cfg.StrOpt('sheepdog_store_address', default=DEFAULT_ADDR,
               help=_('IP address of sheep daemon.'))
]


class SheepdogImage(object):
    """Class describing an image stored in Sheepdog storage."""

    def __init__(self, addr, port, name, chunk_size):
        self.addr = addr
        self.port = port
        self.name = name
        self.chunk_size = chunk_size

    def _run_command(self, command, data, *params):
        cmd = ("dog vdi %(command)s -a %(addr)s -p %(port)d %(name)s "
               "%(params)s" %
               {"command": command,
                "addr": self.addr,
                "port": self.port,
                "name": self.name,
                "params": " ".join(map(str, params))})

        try:
            return processutils.execute(
                cmd, process_input=data, shell=True)[0]
        except processutils.ProcessExecutionError as exc:
            LOG.error(exc)
            raise glance_store.BackendException(exc)

    def get_size(self):
        """
        Return the size of the this iamge

        Sheepdog Usage: dog vdi list -r -a address -p port image
        """
        out = self._run_command("list -r", None)
        return long(out.split(' ')[3])

    def resize(self, size):
        """
        Resize this image to new 'size' in the Sheepdog cluster.

        Sheepdog Usage:
                 dog vdi resize -a address -p port image size
        """
        self._run_command("resize", None, str(size))

    # glance-image is stored in Sheepdog as snapshot, so read from snapshot.
    def read(self, offset, count):
        """
        Read up to 'count' bytes from this image starting at 'offset' and
        return the data.

        Sheepdog Usage:
                 dog vdi read -s snap -a address -p port image offset len
        """
        return self._run_command("read -s %s" % GLANCE_SNAPNAME,
                                 None, str(offset), str(count))

    def write(self, data, offset, count):
        """
        Write up to 'count' bytes from the data to this image starting at
        'offset'

        Sheepdog Usage: dog vdi write -a address -p port image offset len
        """
        self._run_command("write", data, str(offset), str(count))

    def create(self, size):
        """
        Create this image in the Sheepdog cluster with size 'size'.

        Sheepdog Usage: dog vdi create -a address -p port image size
        """
        self._run_command("create", None, str(size))

    def delete(self):
        """
        Delete this image in the Sheepdog cluster

        Sheepdog Usage: dog vdi delete -a address -p port image
        """
        self._run_command("delete", None)

    def create_snapshot(self):
        """
        Create this image in the Sheepdog cluster with size 'size'.

        Sheepdog Usage: dog vdi create -s snap -a address -p port image size
        """
        self._run_command("snapshot -s %s" % GLANCE_SNAPNAME, None)

    def delete_snapshot(self):
        """
        Delete this image from the Sheepdog cluster .

        Sheepdog Usage: dog vdi delete -s snap -a address -p port
        """
        self._run_command("delete -s %s" % GLANCE_SNAPNAME, None)

    def exist(self):
        """
        Check if this image exists in the Sheepdog cluster via 'list' command

        Sheepdog Usage: dog vdi list -r -a address -p port image
        """
        out = self._run_command("list -r", None)
        if not out:
            return False
        else:
            return True


class StoreLocation(glance_store.location.StoreLocation):
    """
    Class describing a Sheepdog URI. This is of the form:

        sheepdog://image

    """

    def process_specs(self):
        self.image = self.specs.get('image')

    def get_uri(self):
        return "sheepdog://%s" % self.image

    def parse_uri(self, uri):
        valid_schema = 'sheepdog://'
        if not uri.startswith(valid_schema):
            reason = _("URI must start with '%s://'") % valid_schema
            raise exceptions.BadStoreUri(message=reason)
        self.image = uri[11:]


class ImageIterator(object):
    """
    Reads data from an Sheepdog image, one chunk at a time.
    """

    def __init__(self, image):
        self.image = image

    def __iter__(self):
        image = self.image
        total = left = image.get_size()
        while left > 0:
            length = min(image.chunk_size, left)
            data = image.read(total - left, length)
            left -= len(data)
            yield data
        raise StopIteration()


class Store(glance_store.driver.Store):
    """Sheepdog backend adapter."""

    _CAPABILITIES = (capabilities.BitMasks.RW_ACCESS |
                     capabilities.BitMasks.DRIVER_REUSABLE)
    OPTIONS = _SHEEPDOG_OPTS
    EXAMPLE_URL = "sheepdog://image"

    def get_schemes(self):
        return ('sheepdog',)

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """

        try:
            chunk_size = self.conf.glance_store.sheepdog_store_chunk_size
            self.chunk_size = chunk_size * units.Mi
            self.READ_CHUNKSIZE = self.chunk_size
            self.WRITE_CHUNKSIZE = self.READ_CHUNKSIZE

            self.addr = self.conf.glance_store.sheepdog_store_address
            self.port = self.conf.glance_store.sheepdog_store_port
        except cfg.ConfigFileValueError as e:
            reason = _("Error in store configuration: %s") % e
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='sheepdog',
                                                   reason=reason)

        try:
            processutils.execute("dog", shell=True)
        except processutils.ProcessExecutionError as exc:
            reason = _("Error in store configuration: %s") % exc
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='sheepdog',
                                                   reason=reason)

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file, and returns a generator for reading
        the image file

        :param location `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises `glance_store.exceptions.NotFound` if image does not exist
        """

        loc = location.store_location
        image = SheepdogImage(self.addr, self.port, loc.image,
                              self.READ_CHUNKSIZE)
        if not image.exist():
            raise exceptions.NotFound(_("Sheepdog image %s does not exist")
                                      % image.name)
        return (ImageIterator(image), image.get_size())

    def get_size(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file and returns the image size

        :param location `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises `glance_store.exceptions.NotFound` if image does not exist
        :rtype int
        """

        loc = location.store_location
        image = SheepdogImage(self.addr, self.port, loc.image,
                              self.READ_CHUNKSIZE)
        if not image.exist():
            raise exceptions.NotFound(_("Sheepdog image %s does not exist")
                                      % image.name)
        return image.get_size()

    @capabilities.check
    def add(self, image_id, image_file, image_size, context=None):
        """
        Stores an image file with supplied identifier to the backend
        storage system and returns a tuple containing information
        about the stored image.

        :param image_id: The opaque image identifier
        :param image_file: The image data to write, as a file-like object
        :param image_size: The size of the image data to write, in bytes

        :retval tuple of URL in backing store, bytes written, and checksum
        :raises `glance_store.exceptions.Duplicate` if the image already
                existed
        :raises `glance_store.exceptions.BackendException` if the image already
                deleted
        """

        image = SheepdogImage(self.addr, self.port, image_id,
                              self.WRITE_CHUNKSIZE)
        if image.exist():
            raise exceptions.Duplicate(image=image_id)

        location = StoreLocation({'image': image_id}, self.conf)
        checksum = hashlib.md5()

        try:
            image.create(image_size)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Fail to create glance-image as Sheepdog VDI. '
                              'src image file: %(image)s, size: %(size)s.'),
                          {'image': image_file, 'size': image_size})

        if image_size == 0:
            try:
                chunks = utils.chunkreadable(image_file, self.WRITE_CHUNKSIZE)
                offset = 0
                for chunk in chunks:
                    image_size += len(chunk)
                    image.resize(image_size)
                    image.write(chunk, offset, len(chunk))
                    checksum.update(chunk)
                    offset += len(chunk)
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error(_LE('Fail to extend image file or '
                                  'write data: %s.'), image_id)
                    image.delete()
        else:
            try:
                total = left = image_size
                while left > 0:
                    length = min(self.chunk_size, left)
                    data = image_file.read(length)
                    image.write(data, total - left, length)
                    left -= length
                    checksum.update(data)
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error(_LE('Fail to read image file or write data '
                                  'src image file: %(image)s, image size: '
                                  '%(size)s Sheepdog VDI name: %(vdiname)s.'),
                              {'image': image_file, 'size': image_size,
                               'vdiname': image_id})
                    image.delete()

        try:
            # create snapshot because images stores snapshot
            image.create_snapshot()
        except Exception:
            # Note(zhiyan): clean up already received data when
            # error occurs such as ImageSizeLimitExceeded exceptions.
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Fail to create Sheepdog snapshot '
                              'Sheepdog VDI name: %s.'), image_id)
                image.delete()

        try:
            # delete current image, use snapshot at sheepdog
            image.delete()
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Fail to delete temporary Sheepdog VDI '
                              'Sheepdog VDI name: %(vdiname)s.'),
                          {'vdiname': image_id})
                image.delete_snapshot()

        return (location.get_uri(), image_size, checksum.hexdigest(), {})

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file to delete

        :location `glance_store.location.Location` object, supplied
                  from glance_store.location.get_location_from_uri()

        :raises NotFound if image does not exist
        """

        loc = location.store_location
        image = SheepdogImage(self.addr, self.port, loc.image,
                              self.WRITE_CHUNKSIZE)
        if not image.exist():
            msg = _("Sheepdog image %s does not exist.") % loc.image
            raise exceptions.NotFound(message=msg)

        # delete the image that was stored as snapshot
        try:
            image.delete_snapshot()
        except Exception:
            with excutils.save_and_reraise_exception():
                # Reraise the original exception
                LOG.error(_LE('Fail to delete a Sheepdog snapshot of '
                              'glance-image. VDI name: %(vdiname)s, '
                              'snapshot name: %(snap)s.'),
                          {'vdiname': image.name, 'snap': GLANCE_SNAPNAME})
