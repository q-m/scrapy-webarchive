from __future__ import annotations

from datetime import datetime

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


@StrategyRegistry.register("before")
class BeforeStrategy(FileLookupStrategy):
    """Strategy to find the file that was last modified before the target time. Should match closest to the target."""

    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        sorted_files = sorted(files, reverse=True)
        target_timestamp = target.timestamp()

        for file in sorted_files:
            if target_timestamp >= file.last_modified:
                return file.uri

        return None


@StrategyRegistry.register("after")
class AfterStrategy(FileLookupStrategy):
    """Strategy to find the file that was last modified after the target time. Should match closest to the target."""

    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        sorted_files = sorted(files)
        target_timestamp = target.timestamp()

        for file in sorted_files:
            if file.last_modified >= target_timestamp:
                return file.uri

        return None
