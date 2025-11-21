#!/usr/bin/env python3
"""
Azure Storage Uploader for Feed Summarizer

This module handles uploading generated RSS feeds and HTML bulletins to Azure Blob Storage.
It checks for file changes and only uploads files that have been modified, and can delete
outdated files from Azure storage that no longer exist locally.
"""

import os
import asyncio
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Set
from datetime import datetime, timezone
from hashlib import md5
import mimetypes

from azure_storage import BlobClient
from config import config, get_logger

# Module-specific logger
logger = get_logger("uploader")

class AzureStorageUploader:
    """Manages uploads to Azure Blob Storage for RSS feeds and HTML bulletins."""
    
    def __init__(self, storage_account: Optional[str] = None, storage_key: Optional[str] = None, container: Optional[str] = None):
        """Initialize the Azure storage uploader.
        
        Args:
            storage_account: Azure storage account name (defaults to config)
            storage_key: Azure storage account key (defaults to config)
            container: Container name (defaults to config, usually '$web')
        """
        self.storage_account = storage_account or config.AZURE_STORAGE_ACCOUNT
        self.storage_key = storage_key or config.AZURE_STORAGE_KEY
        self.container = container or config.AZURE_STORAGE_CONTAINER
        
        self.blob_client = None
        self.enabled = bool(self.storage_account and self.storage_key)
        self._blob_cache: Dict[str, Dict] = {}  # Cache for remote blob info
        
        if not self.enabled:
            logger.info("Azure storage upload disabled - missing storage account or key configuration")
    
    async def initialize(self):
        """Initialize the Azure blob client."""
        if not self.enabled:
            return
            
        try:
            self.blob_client = BlobClient(
                account=self.storage_account,
                auth=self.storage_key
            )
            logger.info(f"Azure storage uploader initialized for account '{self.storage_account}', container '{self.container}'")
        except Exception as e:
            logger.error(f"Failed to initialize Azure blob client: {e}")
            self.enabled = False
    
    async def close(self):
        """Close the Azure blob client."""
        if self.blob_client:
            await self.blob_client.close()
            self.blob_client = None
    
    async def _refresh_blob_cache(self, prefix: Optional[str] = None) -> None:
        """Refresh the cache of remote blob information."""
        if not self.enabled or not self.blob_client:
            return
        
        try:
            logger.info(f"Refreshing blob cache for container '{self.container}'" + (f" with prefix '{prefix}'" if prefix else ""))
            self._blob_cache.clear()
            
            blob_count = 0
            async for blob in self.blob_client.list_blobs(self.container):
                blob_name = blob['name']
                # Filter by prefix if specified
                if prefix and not blob_name.startswith(prefix):
                    continue
                    
                self._blob_cache[blob_name] = blob
                blob_count += 1
            
            logger.info(f"Cached {blob_count} blob(s) from Azure storage")
        except Exception as e:
            logger.error(f"Failed to refresh blob cache: {e}")
            self._blob_cache.clear()
    
    def _get_cached_blob_info(self, blob_path: str) -> Optional[Dict]:
        """Get cached information about a remote blob."""
        return self._blob_cache.get(blob_path)
    
    def _get_local_file_hash(self, file_path: Path) -> Optional[str]:
        """Get MD5 hash of a local file."""
        try:
            with open(file_path, 'rb') as f:
                return md5(f.read()).hexdigest()
        except Exception as e:
            logger.error(f"Failed to hash file {file_path}: {e}")
            return None
    
    def _get_mime_type(self, file_path: Path) -> str:
        """Get MIME type for a file."""
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            if file_path.suffix.lower() == '.xml':
                return 'application/rss+xml'
            elif file_path.suffix.lower() == '.html':
                return 'text/html'
            else:
                return 'application/octet-stream'
        return mime_type
    
    async def _get_remote_blob_info(self, blob_path: str) -> Optional[Dict]:
        """Get information about a remote blob (from cache or by listing)."""
        if not self.enabled or not self.blob_client:
            return None
        
        # First check cache
        cached_info = self._get_cached_blob_info(blob_path)
        if cached_info is not None:
            return cached_info
        
        # If not in cache, do a single blob list operation (fallback)
        try:
            async for blob in self.blob_client.list_blobs(self.container):
                if blob['name'] == blob_path:
                    # Cache this result for future use
                    self._blob_cache[blob_path] = blob
                    return blob
            return None
        except Exception as e:
            logger.debug(f"Could not get remote blob info for {blob_path}: {e}")
            return None
    
    def _should_upload_file(self, local_path: Path, remote_blob_info: Optional[Dict]) -> bool:
        """Determine if a file should be uploaded based on modification time and size."""
        if not local_path.exists():
            return False
            
        if remote_blob_info is None:
            # File doesn't exist remotely, upload it
            return True
        
        local_stat = local_path.stat()
        local_size = local_stat.st_size
        local_mtime = datetime.fromtimestamp(local_stat.st_mtime, tz=timezone.utc)
        
        remote_size = remote_blob_info.get('content-length', 0)
        remote_mtime = remote_blob_info.get('last-modified')
        
        # Upload if size differs or local file is newer
        if local_size != remote_size:
            logger.debug(f"Size difference for {local_path.name}: local={local_size}, remote={remote_size}")
            return True
            
        if remote_mtime and local_mtime > remote_mtime:
            logger.debug(f"Local file newer for {local_path.name}: local={local_mtime}, remote={remote_mtime}")
            return True
            
        return False
    
    async def upload_file(self, local_path: Path, blob_path: str, force: bool = False) -> bool:
        """Upload a single file to Azure storage.
        
        Args:
            local_path: Local file path
            blob_path: Remote blob path
            force: Force upload even if file hasn't changed
            
        Returns:
            True if uploaded, False if skipped or failed
        """
        if not self.enabled or not self.blob_client:
            return False
            
        if not local_path.exists():
            logger.warning(f"Local file does not exist: {local_path}")
            return False
        
        try:
            # Check if upload is needed
            if not force:
                remote_info = await self._get_remote_blob_info(blob_path)
                if not self._should_upload_file(local_path, remote_info):
                    logger.debug(f"Skipping upload of {blob_path} - no changes detected")
                    return False
            
            # Upload the file
            mime_type = self._get_mime_type(local_path)
            cache_control = "public, max-age=300"  # 5 minutes cache for feeds
            
            logger.info(f"Uploading {local_path.name} to {blob_path}")
            
            with open(local_path, 'rb') as f:
                response = await self.blob_client.put_blob(
                    container_name=self.container,
                    blob_path=blob_path,
                    payload=f,
                    mimetype=mime_type,
                    cache_control=cache_control
                )
            
            if response.ok:
                logger.info(f"✅ Successfully uploaded {blob_path}")
                return True
            else:
                logger.error(f"❌ Failed to upload {blob_path}: HTTP {response.status}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error uploading {local_path.name}: {e}")
            return False
    
    async def delete_blob(self, blob_path: str) -> bool:
        """Delete a blob from Azure storage.
        
        Args:
            blob_path: Remote blob path to delete
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.enabled or not self.blob_client:
            return False
        
        try:
            logger.info(f"Deleting blob {blob_path}")
            response = await self.blob_client.delete_blob(self.container, blob_path)
            
            if response.ok:
                logger.info(f"✅ Successfully deleted {blob_path}")
                # Remove from cache
                self._blob_cache.pop(blob_path, None)
                return True
            else:
                logger.error(f"❌ Failed to delete {blob_path}: HTTP {response.status}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error deleting {blob_path}: {e}")
            return False
    
    # ----------------------------
    # Internal helpers (complexity reduction for upload_directory)
    # ----------------------------
    def _list_local_files(self, local_dir: Path, recursive: bool) -> List[Path]:
        """Return list of local files according to recursion flag."""
        if recursive:
            return [f for f in local_dir.rglob("*") if f.is_file()]
        return [f for f in local_dir.glob("*") if f.is_file()]

    def _build_blob_path(self, base_dir: Path, file_path: Path, remote_prefix: str, recursive: bool) -> str:
        """Construct blob path for a local file respecting recursion and prefix."""
        if remote_prefix:
            if recursive:
                rel_path = file_path.relative_to(base_dir).as_posix()
                return f"{remote_prefix.rstrip('/')}/{rel_path}"
            return f"{remote_prefix.rstrip('/')}/{file_path.name}"
        return file_path.name

    def _determine_deletions(
        self,
        expected_blobs: Set[str],
        cache_prefix: Optional[str],
        recursive: bool
    ) -> List[str]:
        """Determine which remote blobs should be deleted during sync.

        Mirrors original logic while being independently testable.
        """
        to_delete: List[str] = []
        for blob_path in self._blob_cache.keys():
            # If a prefix filter exists ensure blob is within it
            if cache_prefix and not blob_path.startswith(cache_prefix):
                continue
            # When no prefix, avoid deleting nested paths
            if not cache_prefix and ('/' in blob_path):
                continue
            # If not recursive, avoid deleting nested blobs below prefix
            if cache_prefix and not recursive:
                rel = blob_path[len(cache_prefix):]
                if '/' in rel:
                    continue
            if blob_path not in expected_blobs:
                to_delete.append(blob_path)
        return to_delete

    async def upload_directory(self, local_dir: Path, remote_prefix: str = "", force: bool = False, sync: bool = False, recursive: bool = False) -> Tuple[int, int, int]:
        """Upload all files in a directory to Azure storage with optional sync.
        
        Args:
            local_dir: Local directory path
            remote_prefix: Remote path prefix
            force: Force upload all files
            sync: If True, delete remote files that don't exist locally
            recursive: If True, include files in subdirectories preserving structure
            
        Returns:
            Tuple of (uploaded_count, skipped_count, deleted_count)
        """
        if not self.enabled:
            return 0, 0, 0
            
        if not local_dir.exists() or not local_dir.is_dir():
            logger.warning(f"Directory does not exist: {local_dir}")
            return 0, 0, 0
        
        # Refresh blob cache for the target prefix
        cache_prefix = remote_prefix.rstrip('/') + '/' if remote_prefix else None
        await self._refresh_blob_cache(cache_prefix)
        
        # Get all local files
        local_files = self._list_local_files(local_dir, recursive)
        
        logger.info(f"Found {len(local_files)} local files in {local_dir}")
        
        uploaded = 0
        skipped = 0
        deleted = 0
        
        # Build set of expected remote blob paths
        expected_blobs: Set[str] = set()
        
        # Upload local files
        for file_path in local_files:
            blob_path = self._build_blob_path(local_dir, file_path, remote_prefix, recursive)
            expected_blobs.add(blob_path)
            if await self.upload_file(file_path, blob_path, force=force):
                uploaded += 1
            else:
                skipped += 1
        
        # If sync is enabled, delete remote blobs that don't exist locally
        if sync:
            remote_blobs_to_delete = self._determine_deletions(expected_blobs, cache_prefix, recursive)
            logger.info(f"Found {len(remote_blobs_to_delete)} remote blobs to delete")
            for blob_path in remote_blobs_to_delete:
                if await self.delete_blob(blob_path):
                    deleted += 1
        
        return uploaded, skipped, deleted
    
    async def sync_public_directory(self, public_dir: Path, force: bool = False, sync: bool = False) -> Dict[str, Tuple[int, int, int]]:
        """Sync the entire public directory to Azure storage.
        
        Args:
            public_dir: Path to public directory
            force: Force upload all files
            sync: If True, delete remote files that don't exist locally (opt-in)
            
        Returns:
            Dictionary with upload results per subdirectory (uploaded, skipped, deleted)
        """
        if not self.enabled:
            return {}
            
        results = {}
        try:
            logger.info(f"Sync root: local='{public_dir}' -> remote container='{self.container}' (account='{self.storage_account}')")
        except Exception:
            pass
        
        # Upload RSS feeds
        feeds_dir = public_dir / "feeds"
        if feeds_dir.exists():
            logger.info("Syncing RSS feeds...")
            # Include subdirectories like feeds/raw by uploading recursively
            results['feeds'] = await self.upload_directory(feeds_dir, "feeds", force=force, sync=sync, recursive=True)
        
        # Upload HTML bulletins
        bulletins_dir = public_dir / "bulletins"
        if bulletins_dir.exists():
            logger.info("Syncing HTML bulletins...")
            results['bulletins'] = await self.upload_directory(bulletins_dir, "bulletins", force=force, sync=sync)
        
        # Upload main index if it exists
        index_file = public_dir / "index.html"
        if index_file.exists():
            logger.info("Syncing main index...")
            # For single file, we'll handle it manually
            await self._refresh_blob_cache()  # Refresh cache for root level
            success = await self.upload_file(index_file, "index.html", force=force)
            
            # If sync is enabled, we might need to clean up other root-level files
            # but we'll be conservative and only manage the specific files we know about
            results['index'] = (1 if success else 0, 0 if success else 1, 0)
        
        return results

    def print_sync_summary(self, results: Dict[str, Tuple[int, int, int]]) -> None:
        """Print a summary of sync results.
        
        Args:
            results: Results dictionary from sync_public_directory
        """
        if not results:
            logger.info("No sync operations performed")
            return
        
        total_uploaded = 0
        total_skipped = 0
        total_deleted = 0
        
        logger.info("📊 Azure Storage Sync Summary:")
        logger.info("=" * 50)
        
        for category, (uploaded, skipped, deleted) in results.items():
            total_uploaded += uploaded
            total_skipped += skipped
            total_deleted += deleted
            
            logger.info(f"{category.upper()}:")
            logger.info(f"  ✅ Uploaded: {uploaded}")
            logger.info(f"  ⏭️  Skipped:  {skipped}")
            logger.info(f"  🗑️  Deleted:  {deleted}")
        
        logger.info("=" * 50)
        logger.info(f"TOTALS - Uploaded: {total_uploaded}, Skipped: {total_skipped}, Deleted: {total_deleted}")
        logger.info("=" * 50)
