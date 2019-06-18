#
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from datetime import datetime
from glob import glob
import logging
import multiprocessing

# We use a multiprocessing Pool for uploads so that we can cancel them with a
# simple SIGTERM, which should bubble down to subprocesses.
from multiprocessing import Pool, current_process

# multiprocessing.dummy is to threads as multiprocessing is to processes.
# Since daemonic processes can't have children, we use a thread to monitor the
# upload pool.
from multiprocessing.dummy import Process

import os
import pickle
import signal
import stat
import time
from uuid import uuid4

from pylorax.sysutils import joinpaths
from pylorax.uploaders import UploaderStatus, DummyUploader
from pylorax.api.queue import uuid_status, uuid_image

# the maximum number of simultaneous uploads
SIMULTANEOUS_UPLOADS = 1

log = logging.getLogger("pylorax")

multiprocessing.log_to_stderr().setLevel(logging.INFO)


def get_queue_path(cfg):
    """Given the composer config, return the upload_queue directory

    :returns: the path to the upload queue
    :rtype: str
    """
    path = joinpaths(cfg.get("composer", "lib_dir"), "upload_queue")
    # create the upload_queue directory if it doesn't exist
    os.makedirs(path, exist_ok=True)
    # make sure the upload_queue directory isn't readable by others, as it will
    # contain sensitive credentials
    current = stat.S_IMODE(os.lstat(path).st_mode)
    os.chmod(path, current & ~stat.S_IROTH)
    return path


class Upload:
    """Wrapper around an Uploader, stores information like a timestamp and a
    UUID. Instances of this class are pickled and stored in the upload_queue
    directory"""

    def __init__(
        self, cfg, uploader_type, compose_uuid, cloud_image_name, image_path, settings
    ):
        self.cfg = cfg
        self.compose_uuid = compose_uuid
        self.uuid = str(uuid4())
        self.timestamp = datetime.now().timestamp()
        self.upload_pid = None
        self.uploader = uploader_type(
            cloud_image_name, image_path, settings, status_callback=self.write
        )
        self.write()

    def summary(self):
        """Return a dict with useful information about the upload

        :returns: upload information
        :rtype: dict
        """
        return {
            "uuid": self.uuid,
            "status": self.uploader.status.value,
            "provider": self.uploader.get_provider(),
            "cloud_image_name": self.uploader.cloud_image_name,
            "compose_uuid": self.compose_uuid,
            "image_path": self.uploader.image_path,
            "image_hash": self.uploader.image_hash,
            "creation_time": self.timestamp,
            "error": self.uploader.error,
        }

    def write(self):
        """Dumps a pickle of the upload to the upload_queue directory"""
        with open(joinpaths(get_queue_path(self.cfg), self.uuid), "wb") as upload_file:
            pickle.dump(self, upload_file, protocol=pickle.HIGHEST_PROTOCOL)

    def execute(self):
        """Starts the upload, meant to be called from a separate process so we
        can cancel it by sending a SIGTERM"""
        self.upload_pid = current_process().pid
        self.uploader.upload()

    def cancel(self):
        """Cancel the upload. Sends a SIGTERM to self.upload_pid"""
        cancellable = frozenset(UploaderStatus.WAITING, UploaderStatus.RUNNING)
        if self.uploader.status not in cancellable:
            raise RuntimeError(f"Can't cancel if status is {self.uploader.status}!")
        if self.upload_pid:
            os.kill(self.upload_pid, signal.SIGTERM)
        self.uploader.set_status(UploaderStatus.CANCELLED)


def start_upload(cfg, compose_uuid, cloud_image_name, settings):
    """Creates a new upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param compose_uuid: the UUID of the compose to upload
    :type compose_uuid: str
    :param cloud_image_name: what to name the image in the cloud provider
    :type cloud_image_name: str
    :param settings: settings to pass to the Uploader
    :type settings: dict
    :returns: the UUID of the created Upload
    :rtype: str
    """

    # For now, infer the target cloud provider from the compose type. We can
    # change this logic later if we want to be able to upload images of the
    # same compose type to different providers
    compose_type = uuid_status(cfg, compose_uuid)["compose_type"]
    uploader_type = {"vmdk": DummyUploader}[  # TODO fill out with other uploaders
        compose_type
    ]
    _, image_path = uuid_image(cfg, compose_uuid)
    upload = Upload(
        cfg, uploader_type, compose_uuid, cloud_image_name, image_path, settings
    )
    return upload.uuid


def list_upload_uuids(cfg):
    """Lists all Upload UUIDs

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :returns: list of Upload UUIDs
    :rtype: list of str
    """
    paths = glob(joinpaths(get_queue_path(cfg), "*"))
    return [os.path.basename(path) for path in paths]


def get_upload(cfg, uuid):
    """Get an Upload object by UUID
    
    :param cfg: the compose config
    :type cfg: ComposerConfig
    """
    with open(joinpaths(get_queue_path(cfg), uuid), "rb") as pickle_file:
        try:
            return pickle.load(pickle_file)
        except:
            return None


def get_all_uploads(cfg):
    """Get all Upload objects

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :returns: a list of all Upload objects
    :rtype: list of Upload
    """
    uploads = [get_upload(cfg, uuid) for uuid in list_upload_uuids(cfg)]
    return [upload for upload in uploads if upload]


def get_upload_summary(cfg, uuid):
    """Return a dict with useful information about the upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload to summarize
    :type uuid: str
    :returns: upload information
    :rtype: dict
    """
    return get_upload(cfg, uuid).summary()


def get_upload_summaries(cfg):
    """Return a list of upload summaries

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :returns: upload information
    :rtype: dict
    """
    return [upload.summary() for upload in get_all_uploads(cfg)]


def cancel_upload(cfg, uuid):
    """Cancel an upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload to cancel
    :type uuid: str
    """
    get_upload(cfg, uuid).cancel()


def get_upload_log(cfg, uuid):
    """Return the log for an upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload
    :type uuid: str
    """
    return get_upload(cfg, uuid).uploader.upload_log


def delete_upload(cfg, uuid):
    """Delete an upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload to delete
    :type uuid: str
    """
    upload = get_upload(cfg, uuid)
    undeletable = frozenset(UploaderStatus.WAITING, UploaderStatus.RUNNING)
    if upload.uploader.status in undeletable:
        raise RuntimeError(f"Can't delete if status is {upload.uploader.status}!")
    os.remove(joinpaths(get_queue_path(cfg), uuid))


def start_upload_monitor(cfg):
    """Start a thread that manages the upload queue

    :param cfg: the compose config
    :type cfg: ComposerConfig
    """
    process = Process(target=monitor, args=(cfg,))
    process.daemon = True
    process.start()


def monitor(cfg):
    """Manage the upload queue

    :param cfg: the compose config
    :type cfg: ComposerConfig
    """
    for upload in get_all_uploads(cfg):
        # Set abandoned uploads to FAILED
        if upload.uploader.status is UploaderStatus.RUNNING:
            upload.uploader.set_status(UploaderStatus.FAILED)
    pool = Pool(processes=SIMULTANEOUS_UPLOADS)
    pool_uuids = set()
    while True:
        # Every second, scoop up WAITING uploads from the filesystem and throw
        # them in the pool
        for upload in get_all_uploads(cfg):
            waiting = upload.uploader.status is UploaderStatus.WAITING
            if waiting and upload.uuid not in pool_uuids:
                pool_uuids.add(upload.uuid)
                pool.apply_async(upload.execute)
        time.sleep(1)
