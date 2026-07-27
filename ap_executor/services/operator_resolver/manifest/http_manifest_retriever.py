from httpx import AsyncClient

from .manifest import OperatorManifest
from .manifest_retriever import ManifestRetriever


class HttpManifestRetriever(ManifestRetriever):

    def __init__(self, http: AsyncClient, manifest_path: str = "/.well-known/operator.json"):
        self._http = http
        self.manifest_path = manifest_path

    async def fetch(self, url: str) -> OperatorManifest:
        resp = await self._http.get(f"{url}{self.manifest_path}", timeout=15.0)
        resp.raise_for_status()
        return OperatorManifest.model_validate(resp.json())
