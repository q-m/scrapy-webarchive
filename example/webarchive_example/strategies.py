from datetime import datetime
from typing import List, Optional
from scrapy_webarchive.models import FileInfo
from scrapy_webarchive.strategies import StrategyRegistry


@StrategyRegistry.register("custom")
class CustomStrategy:
    def find(self, files: List[FileInfo], target: datetime) -> Optional[str]:
        return files[0].uri if files else None
