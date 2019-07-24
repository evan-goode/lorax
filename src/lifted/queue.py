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

from functools import partial
from glob import glob
import logging
import multiprocessing

# We use a multiprocessing Pool for uploads so that we can cancel them with a
# simple SIGINT, which should bubble down to subprocesses.
from multiprocessing import Pool

# multiprocessing.dummy is to threads as multiprocessing is to processes.
# Since daemonic processes can't have children, we use a thread to monitor the
# upload pool.
from multiprocessing.dummy import Process

from operator import attrgetter
import os
import pickle
import stat
import time

from lifted.upload import Upload, UploadStatus
from lifted.providers import (
    resolve_provider,
    resolve_playbook_path,
    validate_settings,
    load_settings,
)

# the maximum number of simultaneous uploads
SIMULTANEOUS_UPLOADS = 1

LOG = logging.getLogger("lifted")
multiprocessing.log_to_stderr().setLevel(logging.INFO)


def _get_queue_path(cfg):
    """Given the upload config, return the upload_queue directory

    :returns: the path to the upload queue
    :rtype: str
    """
    path = cfg["queue_dir"]

    # create the upload_queue directory if it doesn't exist
    os.makedirs(path, exist_ok=True)

    return path


def _get_upload_path(cfg, uuid, write=False):
    path = os.path.join(_get_queue_path(cfg), uuid)
    if write and not os.path.exists(path):
        open(path, "a").close()
    if os.path.exists(path):
        # make sure uploads aren't readable by others, as they will contain
        # sensitive credentials
        current = stat.S_IMODE(os.lstat(path).st_mode)
        os.chmod(path, current & ~stat.S_IROTH)
    return path


def _list_upload_uuids(cfg):
    """Lists all Upload UUIDs

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :returns: list of Upload UUIDs
    :rtype: list of str
    """
    paths = glob(os.path.join(_get_queue_path(cfg), "*"))
    return [os.path.basename(path) for path in paths]


def _write_upload(cfg, upload):
    """Dumps a pickle of the upload to the upload_queue directory"""
    with open(_get_upload_path(cfg, upload.uuid, write=True), "wb") as upload_file:
        pickle.dump(upload, upload_file, protocol=pickle.HIGHEST_PROTOCOL)


def _write_callback(cfg):
    return partial(_write_upload, cfg)


def get_upload(cfg, uuid, ignore_missing=False, ignore_corrupt=False):
    """Get an Upload object by UUID

    :param cfg: the compose config
    :type cfg: ComposerConfig
    """
    try:
        with open(os.path.join(_get_queue_path(cfg), uuid), "rb") as pickle_file:
            return pickle.load(pickle_file)
    except FileNotFoundError as error:
        if not ignore_missing:
            raise RuntimeError(f"Could not find upload {uuid}!") from error
    except pickle.UnpicklingError as error:
        if not ignore_corrupt:
            raise RuntimeError(f"Could not parse upload {uuid}!") from error
    return None


def get_uploads(cfg, uuids):
    uploads = (
        get_upload(cfg, uuid, ignore_missing=True, ignore_corrupt=True)
        for uuid in uuids
    )
    return list(filter(None, uploads))


def get_all_uploads(cfg):
    """Get all Upload objects"""
    return get_uploads(cfg, _list_upload_uuids(cfg))


def create_upload(cfg, provider_name, image_name, settings):
    """Creates a new upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param compose_uuid: the UUID of the compose to upload
    :type compose_uuid: str
    :param image_name: what to name the image in the cloud provider
    :type image_name: str
    :param settings: settings to pass to the Upload
    :type settings: dict
    :returns: the created Upload
    :rtype: str
    """
    validate_settings(cfg, provider_name, settings, image_name)
    return Upload(
        image_name,
        provider_name,
        resolve_playbook_path(cfg, provider_name),
        settings,
        _write_callback(cfg),
    )


def ready_upload(cfg, uuid, image_path):
    """Pass an image_path to an upload and mark it ready to execute"""
    get_upload(cfg, uuid).ready(image_path, _write_callback(cfg))


def reset_upload(cfg, uuid, new_image_name=None, new_settings=None):
    """Reset an upload so it can be attempted again"""
    upload = get_upload(cfg, uuid)
    validate_settings(
        cfg,
        upload.provider_name,
        new_settings or upload.settings,
        new_image_name or upload.image_name,
    )
    if new_image_name:
        upload.image_name = new_image_name
    if new_settings:
        upload.settings = new_settings
    upload.reset(_write_callback(cfg))


def cancel_upload(cfg, uuid):
    """Cancel an upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload to cancel
    :type uuid: str
    """
    get_upload(cfg, uuid).cancel(_write_callback(cfg))


def delete_upload(cfg, uuid):
    """Delete an upload

    :param cfg: the compose config
    :type cfg: ComposerConfig
    :param uuid: the UUID of the upload to delete
    :type uuid: str
    """
    upload = get_upload(cfg, uuid)
    if upload and upload.is_cancellable():
        upload.cancel()
    os.remove(_get_upload_path(cfg, uuid))


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
    LOG.info("Started upload monitor.")
    for upload in get_all_uploads(cfg):
        # Set abandoned uploads to FAILED
        if upload.status is UploadStatus.RUNNING:
            upload.set_status(UploadStatus.FAILED, _write_callback(cfg))
    pool = Pool(processes=SIMULTANEOUS_UPLOADS)
    pool_uuids = set()

    def remover(uuid):
        return lambda _: pool_uuids.remove(uuid)

    while True:
        # Every second, scoop up READY uploads from the filesystem and throw
        # them in the pool
        all_uploads = get_all_uploads(cfg)
        for upload in sorted(all_uploads, key=attrgetter("creation_time")):
            ready = upload.status is UploadStatus.READY
            if ready and upload.uuid not in pool_uuids:
                LOG.info("Starting upload %s...", upload.uuid)
                pool_uuids.add(upload.uuid)
                callback = remover(upload.uuid)
                pool.apply_async(
                    upload.execute,
                    (_write_callback(cfg),),
                    callback=callback,
                    error_callback=callback,
                )
        time.sleep(1)
