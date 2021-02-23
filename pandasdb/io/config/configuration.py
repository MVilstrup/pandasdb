from dataclasses import dataclass, field
from typing import List


@dataclass
class Configuration:
    database: str
    host: str
    password: str
    port: int
    schema: str
    type: str
    username: str

    tunnel: List[str] = field(default=None)
    ssh_username: str = field(default=None)
    ssh_key: str = field(default=None)
    name: str = field(default=None)