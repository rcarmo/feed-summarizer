from base64 import b64decode, b64encode
from email.utils import formatdate, parsedate_to_datetime
from hashlib import sha256
from hmac import HMAC
from io import IOBase
from json import dumps
from typing import Generator, AsyncGenerator, Tuple, List, Optional
from xml.etree import cElementTree
from typing import Union, IO
from time import time

from aiohttp import ClientResponse, ClientSession
from config import get_logger

# Module-specific logger
logger = get_logger("azure")

class BlobClient:
    """Minimal Azure Blob Storage REST API client (SharedKeyLite).

    Only implements the small subset of operations the project actually uses.
    Trimmed to remove unused code and simplify maintenance (dead SearchClient removed).
    """

    account: str | None = None
    auth: bytes | None = None
    session: ClientSession | None = None

    def __init__(self, account: str, auth: str | None = None, session: ClientSession | None = None) -> None:
        assert auth, "Storage account key (auth) is required"
        self.account = account
        self.auth = b64decode(auth)
        self.session = session or ClientSession(json_serialize=dumps)


    async def close(self) -> None:
        """Close the session"""
        await self.session.close()


    def _headers(self, headers: dict | None = None, date: str | None = None) -> dict:
        """Default headers for REST requests"""
        if headers is None:
            headers = {}
        if not date:
            date = formatdate(usegmt=True)  # if you don't use GMT, the API breaks
        return {
            'x-ms-date': date,
            'x-ms-version': '2018-03-28',
            'Content-Type': 'application/octet-stream',
            'Connection': 'Keep-Alive',
            **headers,
        }


    def _sign_for_blobs(self, verb: str, canonicalized: str, headers: dict | None = None, payload: Union[bytes, IO] = b"", length: int | None = None) -> dict:
        """Compute SharedKeyLite authorization header and add standard headers"""
        headers = self._headers(headers)
        signing_headers = sorted(filter(lambda x: 'x-ms' in x, headers.keys()))
        canon_headers = "\n".join("{}:{}".format(k, headers[k]) for k in signing_headers)
        sign = "\n".join([verb, '', headers['Content-Type'], '', canon_headers, canonicalized]).encode('utf-8')
        if length is None and isinstance(payload, IOBase):
            length = payload.seek(0, 2)
            payload.seek(0)
        elif length is None:
            length = len(payload)
        return {
            'Authorization': 'SharedKeyLite {}:{}'.format(self.account, \
                b64encode(HMAC(self.auth, sign, sha256).digest()).decode('utf-8')),
            'Content-Length': str(length),
            **headers
        }


    async def create_container(self, container_name: str) -> ClientResponse:
        """Create a container"""
        canon = f'/{self.account}/{container_name}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container'
        return await self.session.put(uri, headers=self._sign_for_blobs("PUT", canon))


    async def delete_container(self, container_name: str) -> ClientResponse:
        """Delete a container"""
        canon = f'/{self.account}/{container_name}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container'
        return await self.session.delete(uri, headers=self._sign_for_blobs("DELETE", canon))


    async def list_containers(self, marker: str | None = None) -> AsyncGenerator[dict, None]:
        """List containers"""
        canon = f'/{self.account}/?comp=list'
        if marker is None:
            uri = f'https://{self.account}.blob.core.windows.net/?comp=list'
        else:
            uri = f'https://{self.account}.blob.core.windows.net/?comp=list&marker={marker}'

        res = await self.session.get(uri, headers=self._sign_for_blobs("GET", canon))
        if res.ok:
            logger.debug(res.status)
            doc = cElementTree.fromstring(await res.text())
            for container in doc.findall(".//Container"):
                item = {
                    "name": container.find("Name").text
                }
                for prop in container.findall(".//Properties/*"):
                    if prop.tag in ["Creation-Time","Last-Modified","Etag","Content-Length","Content-Type","Content-Encoding","Content-MD5","Cache-Control"]:
                        if prop.tag in ["Last-Modified", "DeletedTime"]:
                            item[prop.tag.lower()] = parsedate_to_datetime(prop.text)
                        else:
                            item[prop.tag.lower()] = prop.text
                yield item
            tag = doc.find("NextMarker")
            if tag is not None and tag.text:
                del res
                del doc
                async for item in self.list_containers(tag.text):
                    yield item
        else:
            logger.error(res.status)
            logger.error(await res.text())


    # ----------------------------------------
    # Internal helpers (blob listing)
    # ----------------------------------------
    def _parse_blob_list_xml(self, xml_text: str) -> Tuple[List[dict], Optional[str]]:
        """Parse Azure List Blobs XML and return (items, next_marker)."""
        doc = cElementTree.fromstring(xml_text)
        items: List[dict] = []
        for blob in doc.findall(".//Blob"):
            item = {"name": blob.findtext("Name")}
            # Only iterate direct Properties children (avoid .// to skip nested duplication)
            props_parent = blob.find("Properties")
            if props_parent is not None:
                for prop in list(props_parent):
                    tag = prop.tag
                    text = prop.text
                    if tag in [
                        "AccessTier","Creation-Time","Last-Modified","Etag","Content-Length",
                        "Content-Type","Content-Encoding","Content-MD5","Cache-Control"
                    ] and text:
                        key = tag.lower()
                        if tag in ["Last-Modified", "Creation-Time"]:
                            item[key] = parsedate_to_datetime(text)
                        elif tag == "Content-Length":
                            item[key] = int(text)
                        elif tag == "Content-MD5":
                            item[key] = b64decode(text.encode('utf-8'))
                        else:
                            item[key] = text
            items.append(item)
        next_marker = doc.findtext("NextMarker") or None
        return items, (next_marker if next_marker else None)

    async def list_blobs(self, container_name: str, marker: str | None = None) -> AsyncGenerator[dict, None]:
        """List blobs (paginated) yielding minimal dict metadata.

        Simplified to a loop + small XML parser helper instead of recursive async calls.
        """
        canon = f'/{self.account}/{container_name}?comp=list'
        base_uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container&comp=list&include=metadata'
        next_marker = marker
        while True:
            uri = base_uri if not next_marker else f"{base_uri}&marker={next_marker}"
            res = await self.session.get(uri, headers=self._sign_for_blobs("GET", canon))
            if not res.ok:
                logger.error(res.status)
                logger.error(await res.text())
                break
            text = await res.text()
            items, next_marker = self._parse_blob_list_xml(text)
            for item in items:
                yield item
            if not next_marker:
                break

  
    async def put_blob(self, container_name: str, blob_path: str, payload: Union[bytes, IO], mimetype: str | None = None, cache_control: str | None = None) -> ClientResponse:
        """Upload a blob"""
        canon = f'/{self.account}/{container_name}/{blob_path}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}/{blob_path}'
        if not mimetype:
            mimetype = "application/octet-stream"
        # note: this is not stored in Azure storage
        etag = b64encode(HMAC((blob_path + str(time())).encode('utf-8'), uri.encode('utf-8'), sha256).digest())
        headers = {
            'x-ms-blob-type': 'BlockBlob',
            'x-ms-blob-content-type': mimetype,
            'Content-Type': mimetype,
            'Etag': f'"{etag}"',
        }
        if cache_control:
            headers['x-ms-blob-cache-control'] = cache_control
        return await self.session.put(uri, data=payload, headers=self._sign_for_blobs("PUT", canon, headers, payload))
        
    async def delete_blob(self, container_name: str, blob_path: str) -> ClientResponse:
        """Delete a blob"""
        canon = f'/{self.account}/{container_name}/{blob_path}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}/{blob_path}'
        return await self.session.delete(uri, headers=self._sign_for_blobs("DELETE", canon))


## Removed unused SearchClient (dead code): functionality not referenced anywhere in the project.
