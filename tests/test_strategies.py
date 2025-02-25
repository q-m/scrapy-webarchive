from datetime import datetime

import pytest
from typing_extensions import List

from scrapy_webarchive.models import FileInfo
from scrapy_webarchive.strategies import AfterStrategy, BeforeStrategy, StrategyRegistry


@pytest.fixture
def sample_files() -> List[FileInfo]:
    return [
        FileInfo("archive_1.wacz", datetime(2025, 1, 1).timestamp()),
        FileInfo("archive_2.wacz", datetime(2025, 6, 1).timestamp()),
        FileInfo("archive_3.wacz", datetime(2025, 12, 1).timestamp()),
    ]


@pytest.mark.parametrize("strategy_name", ["before", "after"])
def test_strategy_registry_register_defaults(strategy_name):
    assert strategy_name in StrategyRegistry._strategies


@pytest.mark.parametrize("strategy_name, strategy_cls", [
    ("before", BeforeStrategy),
    ("after", AfterStrategy),
])
def test_strategy_registry_get_valid(strategy_name, strategy_cls):
    strategy = StrategyRegistry.get(strategy_name)
    assert isinstance(strategy, strategy_cls)


def test_strategy_registry_get_invalid():
    with pytest.raises(ValueError, match="Unknown strategy: invalid"):
        StrategyRegistry.get("invalid")


def test_strategy_registry_new_registration():
    StrategyRegistry._strategies.clear()
    
    class NewStrategy():
        pass
    
    StrategyRegistry.register("new")(NewStrategy)
    assert StrategyRegistry._strategies["new"] == NewStrategy


@pytest.mark.parametrize(
    "strategy, target_date, files_input, expected_result",
    [
        (BeforeStrategy(), datetime(2025, 7, 1), "sample_files", "archive_2.wacz"),
        (BeforeStrategy(), datetime(2024, 12, 1), "sample_files", None),
        (BeforeStrategy(), datetime(2025, 7, 1), [], None),
        (BeforeStrategy(), datetime(2025, 6, 1), "sample_files", "archive_2.wacz"),
        (AfterStrategy(), datetime(2025, 3, 1), "sample_files", "archive_2.wacz"),
        (AfterStrategy(), datetime(2026, 1, 1), "sample_files", None),
        (AfterStrategy(), datetime(2025, 7, 1), [], None),
        (AfterStrategy(), datetime(2025, 6, 1), "sample_files", "archive_2.wacz"),
    ],
    ids=[
        "before_finds_file_before_target",
        "before_no_file_before_target",
        "before_empty_list",
        "before_exact_match",
        "after_finds_file_after_target",
        "after_no_file_after_target",
        "after_empty_list",
        "after_exact_match",
    ]
)
def test_strategy_find(strategy, target_date, files_input, expected_result, sample_files):
    files = sample_files if files_input == "sample_files" else files_input
    result = strategy.find(files, target_date)

    if isinstance(result, dict):
        result = result.get('name')

    assert result == expected_result
