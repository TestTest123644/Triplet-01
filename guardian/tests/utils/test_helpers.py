from datetime import UTC, datetime, timedelta, timezone

from guardian_layer.utils.helpers import epoch_seconds, find_project_root


def test_finds_the_folder_with_pyproject_toml_from_a_nested_folder(tmp_path):
    (tmp_path / "pyproject.toml").touch()
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    assert find_project_root(nested) == tmp_path


def test_finds_a_git_folder_marker(tmp_path):
    (tmp_path / ".git").mkdir()
    nested = tmp_path / "a"
    nested.mkdir()
    assert find_project_root(nested) == tmp_path


def test_nearest_marker_wins(tmp_path):
    (tmp_path / "pyproject.toml").touch()
    inner = tmp_path / "a"
    (inner / "b").mkdir(parents=True)
    (inner / "pyproject.toml").touch()
    assert find_project_root(inner / "b") == inner


def test_returns_none_when_no_marker_exists(tmp_path):
    # a marker nothing has, so the folders above the temp folder cannot match
    assert find_project_root(tmp_path, markers=("no-such-marker",)) is None


def test_default_start_finds_this_repository():
    root = find_project_root()
    assert root is not None
    assert (root / "config" / "settings.yaml").is_file()


def test_epoch_seconds_reads_a_naive_timestamp_as_utc():
    aware = datetime(2026, 10, 2, 0, 0, 5, tzinfo=UTC)
    assert epoch_seconds(aware.replace(tzinfo=None)) == aware.timestamp()


def test_epoch_seconds_keeps_an_explicit_offset():
    plus_one = datetime(2026, 10, 2, 1, 0, 5, tzinfo=timezone(timedelta(hours=1)))
    assert (
        epoch_seconds(plus_one)
        == datetime(2026, 10, 2, 0, 0, 5, tzinfo=UTC).timestamp()
    )
