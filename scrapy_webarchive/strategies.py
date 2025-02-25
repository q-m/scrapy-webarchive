from __future__ import annotations

import importlib
import os
from datetime import datetime
from importlib.util import find_spec

from scrapy.utils.project import ENVVAR
from typing_extensions import Dict, List, Optional, Protocol, Type

from scrapy_webarchive.models import FileInfo


class FileLookupStrategy(Protocol):
    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        """Algorithm to find a file based on a target."""
        ...


class StrategyRegistry:
    _strategies: Dict[str, Type[FileLookupStrategy]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a new strategy."""

        def decorator(strategy_cls: Type[FileLookupStrategy]):
            cls._strategies[name] = strategy_cls
            return strategy_cls  # Ensure the class is returned for normal usage
        return decorator

    @classmethod
    def get(cls, name: str) -> FileLookupStrategy:
        """Retrieves a strategy instance by name."""

        if name not in cls._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        return cls._strategies[name]()
    
    @classmethod
    def auto_discover(cls):
        """Auto-discovers strategies from the current Scrapy project."""

        scrapy_module = os.environ.get(ENVVAR)

        if not scrapy_module:
            return

        project_root = scrapy_module.rsplit(".", 1)[0]
        strategies_module = f"{project_root}.strategies"

        if find_spec(strategies_module):
            importlib.import_module(strategies_module)


@StrategyRegistry.register("before")
class BeforeStrategy:
    """Strategy to find the file that was last modified before the target time. Should match closest to the target."""

    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        sorted_files = sorted(files, reverse=True)
        target_timestamp = target.timestamp()

        for file in sorted_files:
            if target_timestamp >= file.last_modified:
                return file.uri

        return None


@StrategyRegistry.register("after")
class AfterStrategy:
    """Strategy to find the file that was last modified after the target time. Should match closest to the target."""

    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        sorted_files = sorted(files)
        target_timestamp = target.timestamp()

        for file in sorted_files:
            if file.last_modified >= target_timestamp:
                return file.uri

        return None


StrategyRegistry.auto_discover()
