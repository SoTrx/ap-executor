from typing import Dict

from pydantic import BaseModel


class ServiceInstance(BaseModel):
    """A single healthy operator implementation instance resolved from the registry."""
    service_id: str
    service_name: str
    address: str
    port: int
    meta: Dict[str, str] = {}

    @property
    def base_url(self) -> str:
        return f"http://{self.address}:{self.port}"
