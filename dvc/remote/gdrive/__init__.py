from __future__ import unicode_literals

import os
import logging

try:
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
except ImportError:
    GoogleAuth = None
    GoogleDrive = None

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.remote.gdrive.utils import (
    TrackFileReadProgress,
    only_once,
    metadata_isdir,
    shared_token_warning,
)
from dvc.exceptions import DvcException
from dvc.progress import progress


logger = logging.getLogger(__name__)


class GDriveURLInfo(CloudURLInfo):
    @property
    def netloc(self):
        return self.parsed.netloc


class RemoteGDrive(RemoteBASE):
    """Google Drive remote implementation

    ## Some notes on Google Drive design

    Google Drive identifies the resources by IDs instead of paths.

    Folders are regular resources with an `application/vnd.google-apps.folder`
    MIME type. Resource can have multiple parent folders, and also there could
    be multiple resources with the same name linked to a single folder.

    There are multiple root folders accessible from a single user account:
    - `root` (special ID) - alias for the "My Drive" folder
    - `appDataFolder` (special ID) - alias for the hidden application
    space root folder
    - shared drives root folders

    ## Example URLs

    - Datasets/my-dataset inside "My Drive" folder:

        gdrive://root/Datasets/my-dataset

    - Folder by ID (recommended):

        gdrive://1r3UbnmS5B4-7YZPZmyqJuCxLVps1mASC

        (get it https://drive.google.com/drive/folders/{here})

    - Dataset named "my-dataset" in the hidden application folder:

        gdrive://appDataFolder/my-dataset

        (this one wouldn't be visible through Google Drive web UI and
         couldn't be shared)
    """

    scheme = Schemes.GDRIVE
    path_cls = GDriveURLInfo
    REGEX = r"^gdrive://.*$"
    REQUIRES = {"pydrive": GoogleAuth}
    PARAM_CHECKSUM = "md5Checksum"
    DEFAULT_OAUTH_ID = "default"

    # Default credential is needed to show the string of "Data Version
    # Control" in OAuth dialog application name and icon in authorized
    # applications list in Google account security settings. Also, the
    # quota usage is limited by the application defined by client_id.
    # The good practice would be to suggest the user to create their
    # own application credentials.
    DEFAULT_CREDENTIALPATH = os.path.join(
        os.path.dirname(__file__), "google-dvc-client-id.json"
    )
    GOOGLE_AUTH_SETTINGS_PATH = os.path.join(
        os.path.dirname(__file__), "settings.yaml"
    )
    SAVED_USER_CREDENTIALS_FILE = os.path.join(
        os.path.dirname(__file__), "user-credentials"
    )

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.root = self.path_info.netloc.lower()
        self.gdrive = self.drive()

    def drive(self):
        GoogleAuth.DEFAULT_SETTINGS['client_config_backend'] = "settings"
        gauth = GoogleAuth(settings_file=self.GOOGLE_AUTH_SETTINGS_PATH)
        gauth.CommandLineAuth()
        return GoogleDrive(gauth)

    def get_file_checksum(self, path_info):
        file_id = self.get_path_id(path_info)
        gdrive_file = self.gdrive.CreateFile({'id': file_id})
        print("!!!!!Checksum:",gdrive_file['md5Checksum'])
        return gdrive_file['md5Checksum']

    def get_path_id(self, path_info, create=False):
        file_id = ""
        parent_id = path_info.netloc
        file_list = self.gdrive.ListFile({'q': "'%s' in parents and trashed=false" % parent_id}).GetList()
        parts = path_info.path.split("/")
        #print("path parts", parts)
        for part in parts:
            file_id = ""
            for f in file_list:
                if f['title'] == part:
                    #print("Found path part:", part)
                    file_id = f['id']
                    file_list = self.gdrive.ListFile({'q': "'%s' in parents and trashed=false" % file_id}).GetList()
                    parent_id = f['id']
                    break
            if (file_id == ""):
                if create:
                    gdrive_file = self.gdrive.CreateFile({'title': part, "parents" : [{"id" : parent_id}], "mimeType": "application/vnd.google-apps.folder"})
                    gdrive_file.Upload()
                    file_id = gdrive_file['id']
                else:
                    break
        return file_id

    def exists(self, path_info):
        return self.get_path_id(path_info) != ""

    def batch_exists(self, path_infos, callback):
        print("batch_exists check for path info: ", path_infos)
        results = []
        for path_info in path_infos:
            results.append(self.exists(path_info))
            callback.update(str(path_info))
        return results

    def list_cache_paths(self):
        raise DvcException("list_cache_paths my not impl", self.scheme)
        try:
            root = self.gdrive.get_metadata(self.path_info)
        except GDriveResourceNotFound as e:
            logger.debug("list_cache_paths: {}".format(e))
        else:
            prefix = self.path_info.path
            for i in self.gdrive.list_children(root["id"]):
                yield prefix + "/" + i

    @only_once
    def mkdir(self, parent, name):
        raise DvcException("mkdir my not impl", self.scheme)
        return self.gdrive.mkdir(parent, name)

    def makedirs(self, path_info):
        raise DvcException("makedirs my not impl", self.scheme)
        parent = path_info.netloc
        parts = iter(path_info.path.split("/"))
        current_path = ["gdrive://" + path_info.netloc]
        for part in parts:
            try:
                metadata = self.gdrive.get_metadata(
                    self.path_cls.from_parts(
                        self.scheme, parent, path="/" + part
                    )
                )
            except GDriveResourceNotFound:
                break
            else:
                current_path.append(part)
                if not metadata_isdir(metadata):
                    raise GDriveError(
                        "{} is not a folder".format("/".join(current_path))
                    )
                parent = metadata["id"]
        to_create = [part] + list(parts)
        for part in to_create:
            parent = self.mkdir(parent, part)["id"]
        return parent

    def _upload(self, from_file, to_info, name, no_progress_bar):
        print("Upload %s %s %s" % (from_file, to_info, name))
        
        dirname = to_info.parent
        if dirname:
            parent_id = self.get_path_id(dirname, True)
        else:
            parent_id = to_info.netloc

        print("Parent id:", parent_id)
        file1 = self.gdrive.CreateFile({'title': to_info.name, "parents" : [{"id" : parent_id}]})

        from_file = open(from_file, "rb")
        if not no_progress_bar:
            from_file = TrackFileReadProgress(name, from_file)

        file1.content = from_file
        file1.Upload()
        from_file.close()

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self.get_path_id(from_info)
        gdrive_file = self.gdrive.CreateFile({'id': file_id})
        gdrive_file.GetContentFile(to_file)
        if (not no_progress_bar):
            progress.update_target(name, 1, 1)
