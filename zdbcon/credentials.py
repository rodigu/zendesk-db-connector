from dataclasses import dataclass

@dataclass
class Credentials:
    email: str
    token: str
    subdomain: str
