from pandasdb.communication.errors.io import PandasDBConfigurationError
from pandasdb.libraries.configuration.src.base import BaseConfiguration
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class OAuthConfig(BaseConfiguration):

    def __init__(self, default_path, user_path=None):
        self.default_path = default_path
        self.user_path = user_path

    @property
    def key(self):
        if not self.valid:
            self.restart()

        return Credentials.from_authorized_user_file(self.user_path)

    @property
    def valid(self):
        if not os.path.exists(self.default_path):
            raise PandasDBConfigurationError(f"OAuth Configuration file could not be found in {self.default_path}")

        return os.path.exists(self.user_path)

    def restart(self):
        if not os.path.exists(self.user_path):
            flow = InstalledAppFlow.from_client_secrets_file(self.default_path, [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive',
            ])

            user_credentials = flow.run_local_server(port=0)

            with open(self.user_path, 'w') as f:
                f.write(user_credentials.to_json('token'))

        return self