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

import os
import re
import stat

import toml


def resolve_provider(cfg, provider_name):
    path = os.path.join(cfg["providers_dir"], provider_name, "provider.toml")
    try:
        with open(path) as provider_file:
            provider = toml.load(provider_file)
    except OSError as error:
        raise RuntimeError(f'Couldn\'t find provider "{provider_name}"!') from error
    return provider


def resolve_playbook_path(cfg, provider_name):
    path = os.path.join(cfg["providers_dir"], provider_name, "playbook.yaml")
    if not os.path.isfile(path):
        raise RuntimeError(f'Couldn\'t find playbook for "{provider_name}"!')
    return path


def _get_settings_path(cfg, provider_name, write=False):
    directory = cfg["settings_dir"]

    # create the upload_queue directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)

    path = os.path.join(directory, f"{provider_name}.toml")
    if write and not os.path.isfile(path):
        open(path, "a").close()
    if os.path.exists(path):
        # make sure settings files aren't readable by others, as they will contain
        # sensitive credentials
        current = stat.S_IMODE(os.lstat(path).st_mode)
        os.chmod(path, current & ~stat.S_IROTH)
    return path


def get_settings_info(cfg, provider_name):
    provider = resolve_provider(cfg, provider_name)
    settings_info = provider["settings-info"]
    saved_settings = load_settings(cfg, provider_name)
    for key, info in settings_info.items():
        info["value"] = saved_settings[key] if key in saved_settings else ""
    return settings_info


def load_settings(cfg, provider_name):
    path = _get_settings_path(cfg, provider_name, write=False)
    if os.path.isfile(path):
        with open(path) as settings_file:
            return toml.load(settings_file)
    return {}


def validate_settings(cfg, provider_name, settings):
    settings_info = get_settings_info(cfg, provider_name)
    for key, value in settings.items():
        if key not in settings_info:
            raise RuntimeError(f'Received unexpected setting: "{key}"!')
        if "regex" in settings_info[key]:
            if not re.match(settings_info[key]["regex"], value):
                raise RuntimeError(f'Value "{value}" is invalid for setting "{key}"!')


def save_settings(cfg, provider_name, settings):
    validate_settings(cfg, provider_name, settings)
    with open(_get_settings_path(cfg, provider_name, write=True), "w") as settings_file:
        toml.dump(settings, settings_file)
