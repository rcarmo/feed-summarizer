#!/usr/bin/env python3
"""
Master Feed Processing Orchestrator

This script orchestrates the entire feed processing pipeline in the correct sequence:
1. Fetch new RSS items from feeds
2. Generate AI summaries for new items
3. Publish HTML bulletins from summaries
4. Generate RSS feeds from bulletins

Uses direct imports and function calls for efficient, reliable pipeline execution.
Supports single-run mode for manual execution and scheduled mode for automated processing.
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import sqlite3
import argparse

# Import the main functions from each module
from fetcher import FeedFetcher
from summarizer import NewsProcessor
from publisher import RSSPublisher
from config import config, get_logger
from scheduler import create_scheduler
from telemetry import init_telemetry, get_tracer, trace_span

# Module-specific logger
logger = get_logger("orchestrator")
init_telemetry("feed-summarizer-orchestrator")
_tracer = get_tracer("orchestrator")

class FeedProcessingOrchestrator:
    """Orchestrates the feed processing pipeline."""
    
    def __init__(self, workspace_path: Optional[str] = None) -> None:
        """Initialize the orchestrator.
        
        Args:
            workspace_path: Path to the workspace directory. If None, uses current directory.
        """
        self.workspace_path = Path(workspace_path) if workspace_path else Path.cwd()
        
    async def run_fetcher(self, only_slugs: Optional[list] = None) -> bool:
        """Run the feed fetcher step."""
        logger.info("📡 Running feed fetcher")
        try:
            return await self._run_fetcher_impl(only_slugs)
        except Exception as e:
            logger.error(f"❌ Feed fetcher failed: {e}")
            return False

    @trace_span("run_fetcher", tracer_name="orchestrator", attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""})
    async def _run_fetcher_impl(self, only_slugs: Optional[list] = None) -> bool:
        fetcher = FeedFetcher()
        await fetcher.initialize()
        await fetcher.fetch_all_feeds(only_slugs=only_slugs)
        await fetcher.close()
        logger.info("✅ Feed fetcher completed successfully")
        return True
    
    async def run_summarizer(self, only_slugs: Optional[list] = None) -> bool:
        """Run the summarizer step."""
        logger.info("🧠 Running summarizer")
        try:
            return await self._run_summarizer_impl(only_slugs)
        except Exception as e:
            logger.error(f"❌ Summarizer failed: {e}")
            return False

    @trace_span("run_summarizer", tracer_name="orchestrator", attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""})
    async def _run_summarizer_impl(self, only_slugs: Optional[list] = None) -> bool:
        processor = NewsProcessor()
        await processor.initialize()
        await processor.process_all_feeds(only_slugs=only_slugs)
        await processor.close()
        logger.info("✅ Summarizer completed successfully")
        return True
    
    async def run_html_publisher(self, enable_azure_upload: bool = True, only_slugs: Optional[list] = None) -> bool:
        """Run the HTML publisher step."""
        logger.info("📄 Running HTML publisher")
        try:
            return await self._run_html_publisher_impl(enable_azure_upload, only_slugs)
        except Exception as e:
            logger.error(f"❌ HTML publisher failed: {e}")
            return False

    @trace_span("run_html_publisher", tracer_name="orchestrator", attr_from_args=lambda self, enable_azure_upload=True, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else "", "azure.upload.enabled": bool(enable_azure_upload)})
    async def _run_html_publisher_impl(self, enable_azure_upload: bool = True, only_slugs: Optional[list] = None) -> bool:
        publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=enable_azure_upload)
        await publisher.initialize()
        html_count = await publisher.publish_all_html_bulletins(only_slugs=only_slugs)
        await publisher.close()
        logger.info(f"✅ HTML publisher completed successfully ({html_count} bulletins)")
        return True
    
    async def run_rss_publisher(self, enable_azure_upload: bool = True, only_slugs: Optional[list] = None) -> bool:
        """Run the RSS publisher step."""
        logger.info("📡 Running RSS publisher")
        try:
            return await self._run_rss_publisher_impl(enable_azure_upload, only_slugs)
        except Exception as e:
            logger.error(f"❌ RSS publisher failed: {e}")
            return False

    @trace_span("run_rss_publisher", tracer_name="orchestrator", attr_from_args=lambda self, enable_azure_upload=True, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else "", "azure.upload.enabled": bool(enable_azure_upload)})
    async def _run_rss_publisher_impl(self, enable_azure_upload: bool = True, only_slugs: Optional[list] = None) -> bool:
        publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=enable_azure_upload)
        await publisher.initialize()
        rss_count = await publisher.publish_all_rss_feeds(only_slugs=only_slugs)
        pt_count = await publisher.publish_passthrough_feeds()
        await publisher._write_index_html()
        logger.info(f"✅ RSS publisher completed successfully ({rss_count} summary feeds, {pt_count} passthrough feeds)")
        return True

    @trace_span("run_passthrough_publisher", tracer_name="orchestrator", attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""})
    async def run_passthrough_publisher(self, only_slugs: Optional[list] = None) -> bool:
        """Publish passthrough feeds only (no summaries/bulletins)."""
        logger.info("📡 Running passthrough RSS publisher")
        try:
            publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=False)
            await publisher.initialize()
            count = await publisher.publish_passthrough_feeds(only_slugs=only_slugs)
            await publisher._write_index_html()
            await publisher.close()
            logger.info(f"✅ Passthrough publisher completed successfully ({count} feeds)")
            return True
        except Exception as e:
            logger.error(f"❌ Passthrough publisher failed: {e}")
            return False
    
    async def run_publisher_all(self, enable_azure_upload: bool = True) -> bool:
        """Run unified publisher (HTML + RSS + indexes + passthrough)."""
        logger.info("📰 Running unified publisher (HTML + RSS)")
        try:
            publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=enable_azure_upload)
            await publisher.initialize()
            # Publish everything (HTML bulletins -> RSS feeds -> passthrough -> indexes)
            await publisher.publish_all_content()
            await publisher.close()
            logger.info("✅ Unified publisher completed successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Unified publisher failed: {e}")
            return False

    async def run_publisher_with_upload(self, force_upload: bool = False, sync_delete: Optional[bool] = None) -> bool:
        """Upload existing published content to Azure without re-publishing.

        Args:
            force_upload: Force upload all files to Azure even if unchanged

        Returns:
            True if successful, False otherwise
        """
        logger.info("📤 Upload-only mode: syncing existing content to Azure")
        logger.info(f"Paths: DATA_PATH={config.DATA_PATH} PUBLIC_DIR={config.PUBLIC_DIR} DATABASE_PATH={config.DATABASE_PATH}")
        try:
            publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=True)
            await publisher.initialize()

            # Default to forced upload to guarantee freshness
            results = await publisher.upload_to_azure(force=True, sync_delete=sync_delete)

            await publisher.close()

            if results:
                total_uploaded = sum(uploaded for uploaded, _, _ in results.values())
                total_skipped = sum(skipped for _, skipped, _ in results.values())
                total_deleted = sum(deleted for _, _, deleted in results.values())
                logger.info(f"✅ Azure upload complete: {total_uploaded} uploaded, {total_skipped} skipped, {total_deleted} deleted")
            else:
                logger.info("ℹ️ Azure upload skipped or not configured")

            return True
        except Exception as e:
            logger.error(f"❌ Upload step failed: {e}")
            return False

    @trace_span(
        "pipeline.run",
        tracer_name="orchestrator",
        attr_from_args=lambda self, publish_content=True, only_slugs=None: {
            "pipeline.publish": bool(publish_content),
            "feed.only_slugs": ",".join(only_slugs) if only_slugs else "",
        },
    )
    async def run_pipeline(self, publish_content: bool = True, only_slugs: Optional[list] = None, enable_azure_upload: bool = True, sync_delete: Optional[bool] = None) -> bool:
        """Run the complete feed processing pipeline.
        
        Args:
            publish_content: Whether to run the publishing steps (HTML/RSS generation)
            only_slugs: If provided, limit fetch/summarize to these feed slugs
            enable_azure_upload: If True, upload generated content to Azure after publishing
            
        Returns:
            True if all steps succeeded, False otherwise
        """
        logger.info("🚀 Starting feed processing pipeline")
        logger.info(f"Paths: DATA_PATH={config.DATA_PATH} PUBLIC_DIR={config.PUBLIC_DIR} DATABASE_PATH={config.DATABASE_PATH}")
        start_time = time.time()
        # Step 1: Fetch feeds
        if not await self.run_fetcher(only_slugs=only_slugs):
            logger.error("💀 Critical step failed, stopping pipeline")
            return False

        # Step 2: Generate summaries
        if not await self.run_summarizer(only_slugs=only_slugs):
            logger.error("💥 Critical step failed, stopping pipeline")
            return False

        # Step 3 & 4: Publish content (optional). Publish both HTML and RSS in one go.
        if publish_content:
            pub_success = await self.run_publisher_all()
            if not pub_success:
                logger.warning("⚠️ Publishing step failed, but pipeline continued")

        # Always update passthrough feeds for relevant slugs, even in per-feed runs without publishing summaries
        try:
            await self.run_passthrough_publisher(only_slugs=only_slugs)
        except Exception:
            pass

        # Step 5: Upload to Azure (default on) after all publishing steps
        if enable_azure_upload:
            try:
                logger.info("🌥️ Uploading generated content to Azure (forced)")
                publisher = RSSPublisher(base_url=config.RSS_BASE_URL, enable_azure_upload=True)
                await publisher.initialize()
                results = await publisher.upload_to_azure(force=True, sync_delete=sync_delete)
                await publisher.close()

                if results:
                    total_uploaded = sum(uploaded for uploaded, _, _ in results.values())
                    total_skipped = sum(skipped for _, skipped, _ in results.values())
                    total_deleted = sum(deleted for _, _, deleted in results.values())
                    logger.info(f"✅ Azure upload complete: {total_uploaded} uploaded, {total_skipped} skipped, {total_deleted} deleted")
                else:
                    logger.info("ℹ️ Azure upload skipped or not configured")
            except Exception as e:
                logger.error(f"❌ Azure upload step failed: {e}")
        else:
            # Provide explicit reason for skipping upload to aid diagnostics
            if not publish_content:
                logger.info("ℹ️ Azure upload skipped: publishing disabled for this run")
            elif not enable_azure_upload:
                logger.info("ℹ️ Azure upload skipped: disabled via --no-azure or configuration flag")

        # Pipeline summary
        elapsed_time = time.time() - start_time
        logger.info(f"🎉 Pipeline completed successfully in {elapsed_time:.1f}s")
        return True
    
    def check_status(self) -> dict:
        """Check the current status of the feed processing system.
        
        Returns:
            Dictionary with status information
        """
        logger.info("📊 Checking system status")
        
        status = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'checks': {}
        }
        
        # Check database
        try:
            db_path = Path(config.DATABASE_PATH)
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Count total items
                cursor.execute("SELECT COUNT(*) FROM items")
                total_items = cursor.fetchone()[0]
                
                # Count summaries
                cursor.execute("SELECT COUNT(*) FROM summaries")
                total_summaries = cursor.fetchone()[0]
                
                # Count unpublished summaries
                cursor.execute("SELECT COUNT(*) FROM summaries WHERE published_date IS NULL")
                unpublished_summaries = cursor.fetchone()[0]
                
                # Count bulletins
                cursor.execute("SELECT COUNT(*) FROM bulletins")
                total_bulletins = cursor.fetchone()[0]
                
                conn.close()
                
                status['checks']['database'] = {
                    'status': 'ok',
                    'total_items': total_items,
                    'total_summaries': total_summaries,
                    'unpublished_summaries': unpublished_summaries,
                    'total_bulletins': total_bulletins,
                    'summarization_rate': f"{(total_summaries/total_items*100):.1f}%" if total_items > 0 else "0%"
                }
            else:
                status['checks']['database'] = {
                    'status': 'missing',
                    'message': 'Database file not found'
                }
        except Exception as e:
            status['checks']['database'] = {
                'status': 'error',
                'message': str(e)
            }
        
        # Check output directories (respect configured PUBLIC_DIR)
        public_dir = Path(config.PUBLIC_DIR)
        status['checks']['output'] = {
            'public_dir_exists': public_dir.exists(),
            'html_bulletins': len(list((public_dir / 'bulletins').glob('*.html'))) if public_dir.exists() else 0,
            'rss_feeds': len(list((public_dir / 'feeds').glob('*.xml'))) if public_dir.exists() else 0,
        }
        
        # Summary
        all_ok = (
            status['checks']['database'].get('status') == 'ok' and
            status['checks']['output']['public_dir_exists']
        )
        status['overall_status'] = 'healthy' if all_ok else 'issues_detected'
        
        return status
    
    def print_status(self, status: dict):
        """Print formatted status information."""
        print(f"\n📊 Feed Processing System Status")
        print(f"⏰ {status['timestamp']}")
        print(f"🏥 Overall: {status['overall_status'].upper()}")
        
        # Database status
        db = status['checks']['database']
        if db['status'] == 'ok':
            print(f"\n💾 Database:")
            print(f"   📰 Items: {db['total_items']}")
            print(f"   📝 Summaries: {db['total_summaries']} ({db['summarization_rate']})")
            print(f"   ⏳ Unpublished: {db['unpublished_summaries']}")
            print(f"   📋 Bulletins: {db['total_bulletins']}")
        else:
            print(f"\n💾 Database: {db['status'].upper()} - {db.get('message', 'Unknown error')}")
        
        # Output status
        output = status['checks']['output']
        print(f"\n📁 Output:")
        print(f"   📄 HTML Bulletins: {output['html_bulletins']}")
        print(f"   📡 RSS Feeds: {output['rss_feeds']}")


async def run_scheduled_mode():
    """Run the orchestrator in smart scheduled mode using feeds.yaml configuration."""
    
    orchestrator = FeedProcessingOrchestrator()
    scheduler = create_scheduler()
    
    logger.info("🕐 Starting smart scheduled mode using feeds.yaml schedule configuration")
    
    # Check if schedule is configured
    status = scheduler.get_schedule_status()
    if not status['schedule_active']:
        logger.error("❌ No schedule configured in feeds.yaml")
        logger.info("💡 Please add a 'schedule' section to feeds.yaml with time entries")
        logger.info("� Example:\n   schedule:\n     - time: \"06:30\"\n     - time: \"12:30\"\n     - time: \"20:30\"")
        return
    
    # Run the scheduled pipeline
    await scheduler.run_scheduled_pipeline(orchestrator)


def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(description='Feed Processing Orchestrator')
    parser.add_argument('mode', choices=['run', 'status', 'scheduled', 'upload', 'schedule-status', 'fetcher', 'summarizer', 'publish'], 
                       help='Operation mode')
    parser.add_argument('--no-publish', action='store_true',
                       help='Skip publishing steps (HTML/RSS generation)')
    parser.add_argument('--force-upload', action='store_true',
                       help='Force upload all files to Azure even if unchanged')
    parser.add_argument('--no-azure', action='store_true',
                       help='Disable Azure storage upload during run (default is upload-after-publish)')
    parser.add_argument('--sync-delete', action='store_true',
                       help='When uploading to Azure, delete remote files not present locally (dangerous unless you know what you are doing)')
    parser.add_argument('--workspace', type=str,
                       help='Workspace directory path')
    
    args = parser.parse_args()
    
    orchestrator = FeedProcessingOrchestrator(args.workspace)
    
    try:
        if args.mode == 'run':
            # Single pipeline run
            success = asyncio.run(orchestrator.run_pipeline(publish_content=not args.no_publish, enable_azure_upload=not args.no_azure, sync_delete=args.sync_delete))
            sys.exit(0 if success else 1)
            
        elif args.mode == 'status':
            # Check and display status
            status = orchestrator.check_status()
            orchestrator.print_status(status)
            
        elif args.mode == 'scheduled':
            # Run in scheduled mode
            asyncio.run(run_scheduled_mode())
            
        elif args.mode == 'upload':
            # Run publisher with Azure upload only
            success = asyncio.run(orchestrator.run_publisher_with_upload(force_upload=args.force_upload, sync_delete=args.sync_delete))
            sys.exit(0 if success else 1)
            
        elif args.mode == 'schedule-status':
            # Show schedule status
            scheduler = create_scheduler()
            scheduler.print_schedule_status()
        
        elif args.mode == 'fetcher':
            # Run only the fetcher step
            success = asyncio.run(orchestrator.run_fetcher())
            sys.exit(0 if success else 1)
        
        elif args.mode == 'summarizer':
            # Run only the summarizer step
            success = asyncio.run(orchestrator.run_summarizer())
            sys.exit(0 if success else 1)

        elif args.mode == 'publish':
            # Publish HTML + RSS + indexes using existing data (no fetch/summarize)
            success = asyncio.run(orchestrator.run_publisher_all(enable_azure_upload=not args.no_azure))
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        logger.info("👋 Orchestrator shutting down")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
