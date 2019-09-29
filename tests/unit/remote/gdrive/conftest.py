from datetime import datetime, timedelta
import json
import mock

from google_auth_oauthlib.flow import InstalledAppFlow
import google.oauth2.credentials

import pytest

from dvc.repo import Repo
from dvc.remote.gdrive import RemoteGDrive
from dvc.remote.gdrive.utils import MIME_GOOGLE_APPS_FOLDER


AUTHORIZATION = {"authorization": "Bearer MOCK_token"}
FOLDER = {"mimeType": MIME_GOOGLE_APPS_FOLDER}
FILE = {"mimeType": "not-a-folder"}


class Response:
    def __init__(self, data, status_code=200):
        self._data = data
        self.text = json.dumps(data) if isinstance(data, dict) else data
        self.status_code = status_code

    def json(self):
        return self._data


@pytest.fixture()
def repo():
    return Repo(".")


@pytest.fixture
def gdrive(repo):
    ret = RemoteGDrive(repo, {"url": "gdrive://root/data"})
    return ret


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    mocked = mock.Mock(return_value=Response("test"))
    monkeypatch.setattr("requests.sessions.Session.request", mocked)
    return mocked


def _p(root, path):
    return RemoteGDrive.path_cls.from_parts(
        "gdrive", netloc=root, path="/" + path
    )
