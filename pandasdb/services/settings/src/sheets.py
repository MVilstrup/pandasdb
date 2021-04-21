import os

from pandasdb.libraries.configuration.src.oauth_config import OAuthConfig


def sheets_settings():
    return {
        "gsuite": OAuthConfig(
            default_path=os.path.expanduser(os.path.join("~", ".pandas_db", "plugins", "gsuite", "credentials.json")),
            user_path=os.path.expanduser(os.path.join("~", ".pandas_db", "plugins", "gsuite", "authorized_user.json"))
        )
    }
