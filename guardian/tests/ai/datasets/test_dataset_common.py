import csv
import gzip
import io
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets import common
from guardian_layer.ai.datasets.common import (
    dataset_path,
    in_sampled_block,
    open_text,
    parse_float,
    sampled_lines,
    to_readings,
    to_utc,
    zip_names,
)
from guardian_layer.utils.dataset_config import DatasetSettings
from guardian_layer.utils.schema import Anomalies, DatasetRow


def test_relative_root_is_joined_to_the_project_root(tmp_path, monkeypatch):
    (tmp_path / "repo").mkdir()  # Linux resolves ".." only through a real folder
    monkeypatch.setattr(common, "find_project_root", lambda: tmp_path / "repo")
    target = tmp_path / "data" / "nab" / "x.csv"
    target.parent.mkdir(parents=True)
    target.write_text("")
    path = dataset_path(DatasetSettings(root="../data"), "nab", "x.csv")
    assert path.resolve() == target.resolve()


def test_absolute_root_is_used_as_is(tmp_path):
    (tmp_path / "nab").mkdir()
    (tmp_path / "nab" / "x.csv").write_text("")
    path = dataset_path(DatasetSettings(root=str(tmp_path)), "nab", "x.csv")
    assert path == tmp_path / "nab" / "x.csv"


def test_missing_file_names_the_path_and_the_setting(tmp_path):
    with pytest.raises(FileNotFoundError, match=r"datasets\.root") as error:
        dataset_path(DatasetSettings(root=str(tmp_path)), "nab", "x.csv")
    assert "x.csv" in str(error.value)


def test_open_text_reads_plain_and_gzip(tmp_path):
    plain = tmp_path / "x.txt"
    plain.write_bytes(b"a\nb\n")  # bytes: write_text would write CRLF on Windows
    packed = tmp_path / "x.txt.gz"
    packed.write_bytes(gzip.compress(b"a\nb\n"))
    for path in (plain, packed):
        with open_text(path) as file:
            assert list(file) == ["a\n", "b\n"]


def test_to_utc_reads_naive_as_utc():
    assert to_utc("2004-02-28 01:03:16") == datetime(2004, 2, 28, 1, 3, 16, tzinfo=UTC)


def test_to_utc_converts_an_aware_time():
    assert to_utc("2020-01-01T01:00:00+01:00") == datetime(2020, 1, 1, tzinfo=UTC)


def test_to_utc_with_a_format():
    expected = datetime(2018, 1, 1, 0, 0, 6, tzinfo=UTC)
    assert to_utc("2018/01/01 00:00:06", "%Y/%m/%d %H:%M:%S") == expected


@pytest.mark.parametrize("text", ["", " ", "NA", "?", "nan", "inf", "-inf", "abc"])
def test_parse_float_rejects_missing_and_non_finite(text):
    assert parse_float(text, missing=("?",)) is None


def test_parse_float_rejects_a_listed_sentinel():
    assert parse_float("-200", missing=("-200",)) is None


def test_parse_float_reads_a_number():
    assert parse_float(" 19.9884 ") == 19.9884


def write_zip(path, members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    path.write_bytes(buffer.getvalue())
    return buffer.getvalue()


def test_open_text_reads_a_zip_member(tmp_path):
    path = tmp_path / "a.zip"
    write_zip(path, {"x.csv": b"a\nb\n"})
    with open_text(path, "x.csv") as file:
        assert list(file) == ["a\n", "b\n"]


def test_open_text_reads_a_member_of_a_nested_zip(tmp_path):
    inner = write_zip(tmp_path / "inner.zip", {"d/x.csv": b"a\nb\n"})
    path = tmp_path / "outer.zip"
    write_zip(path, {"inner.zip": inner})
    with open_text(path, "inner.zip", "d/x.csv") as file:
        assert list(file) == ["a\n", "b\n"]


def test_open_text_strips_a_bom(tmp_path):
    path = tmp_path / "x.csv"
    path.write_bytes(b"\xef\xbb\xbfid;v\n1;2\n")
    with open_text(path) as file:
        assert next(csv.DictReader(file, delimiter=";"))["id"] == "1"


def test_open_text_missing_member_raises(tmp_path):
    path = tmp_path / "a.zip"
    write_zip(path, {"x.csv": b""})
    with pytest.raises(KeyError):
        open_text(path, "y.csv")
    path.unlink()  # the zip was closed: Windows refuses to delete an open file


def test_zip_names_skips_folders_and_macosx(tmp_path):
    members = {"d/": b"", "d/x.csv": b"", "__MACOSX/._x.csv": b""}
    inner = write_zip(tmp_path / "inner.zip", members)
    write_zip(tmp_path / "outer.zip", {"inner.zip": inner})
    assert zip_names(tmp_path / "inner.zip") == ["d/x.csv"]
    assert zip_names(tmp_path / "outer.zip", "inner.zip") == ["d/x.csv"]


def row(hour, station="s", kind="k", value=1.0, **extra):
    time = datetime(2020, 1, 1, hour, tzinfo=UTC)
    return DatasetRow(time, station, kind, "u", value, **extra)


def test_to_readings_builds_ids_and_orders():
    rows = [
        row(1, lat=1.0, lon=2.0),
        row(2, label=Anomalies.OUT_OF_RANGE),
        row(1, station="t"),
    ]
    readings = list(to_readings("src", rows))
    assert [r.sensor.sensor_id for r in readings] == ["src:s:k", "src:s:k", "src:t:k"]
    assert {r.sensor.cluster_id for r in readings} == {"src:k"}
    orders = [r.sensor.metadata.packet_order for r in readings if r.sensor.metadata]
    assert orders == [1, 2, 1]
    first = readings[0].sensor.metadata
    assert first is not None
    assert (first.lat, first.lon, first.type) == (1.0, 2.0, "k")
    assert readings[0].sensor.unit == "u"
    assert readings[1].label is Anomalies.OUT_OF_RANGE


def test_to_readings_sorts_when_asked():
    rows = [row(12, value=3.0), row(11, value=2.0), row(10, value=1.0)]
    readings = list(to_readings("src", rows, sort=True))
    assert [r.sensor.value for r in readings] == [1.0, 2.0, 3.0]
    orders = [r.sensor.metadata.packet_order for r in readings if r.sensor.metadata]
    assert orders == [1, 2, 3]
    # unsorted, a newest-first source keeps only what never steps back in time
    kept = list(to_readings("src", rows))
    assert [r.sensor.value for r in kept] == [3.0]


@pytest.mark.parametrize(
    ("text", "missing"),
    [("-200.0", ("-200",)), ("-100000.00", ("-100000",)), ("?", ("?",))],
)
def test_parse_float_matches_a_sentinel_by_value(text, missing):
    # UCI writes its -200 sentinel as -200,0 too; Chicago writes -100000.00
    assert parse_float(text, missing=missing) is None


def test_to_readings_drops_repeated_and_earlier_times():
    # a source that repeats a block of rows must not step back in time
    rows = [
        row(10, value=1.0),
        row(11, value=2.0),
        row(11, value=9.0),
        row(10, value=9.0),
    ]
    readings = list(to_readings("src", rows))
    assert [r.sensor.value for r in readings] == [1.0, 2.0]


def test_to_readings_carries_the_event_marker():
    rows = [row(1, event=True), row(2)]
    readings = list(to_readings("src", rows))
    assert [r.event for r in readings] == [True, False]
    assert all(r.label is None for r in readings)  # a marker, not a label


def test_in_sampled_block_keeps_block_hours_out_of_every():
    kept = [h for h in range(30) if in_sampled_block(h * 3600 + 59, 29, 2)]
    assert kept == [0, 1, 29]
    assert in_sampled_block(12345.0, 1, 2)  # every hour: keep everything


def test_sampled_lines_keeps_whole_hours_and_unparseable_lines():
    lines = [
        "2019/01/01 00:10:00,a",
        "2019/01/01 01:59:59,b",
        "2019/01/01 02:00:00,c",
        "garbage",
    ]
    kept = list(sampled_lines(lines, 3, 2, "%Y/%m/%d %H"))
    assert kept == [lines[0], lines[1], "garbage"]


def test_sampled_lines_parses_each_prefix_once(monkeypatch):
    calls = []
    real = common.datetime

    class Counting:
        @classmethod
        def strptime(cls, text, fmt):
            calls.append(text)
            return real.strptime(text, fmt)

    monkeypatch.setattr(common, "datetime", Counting)
    lines = [f"2019/01/01 00:00:{i % 60:02d},{i}" for i in range(1000)]
    assert len(list(sampled_lines(lines, 3, 2, "%Y/%m/%d %H"))) == 1000
    assert calls == ["2019/01/01 00"]
