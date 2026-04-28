import pytest

from mqtt325.models import Retainer


def test_no_identical_in_out_topics():
    with pytest.raises(ValueError, match="must be different"):
        Retainer("foo/+/sub/#", "foo/+/sub/#")
    pass


def test_with_clientid_proxying():
    r = Retainer(
        input_topic="some/+/subtopic",
        output_topic="proxied/some/+/subtopic",
    )
    assert (
        r.to_output_topic("some/client_id/subtopic")
        == "proxied/some/client_id/subtopic"
    )
    pass


def test_withclientid_level_reorder():
    r = Retainer(
        input_topic="some/+/subtopic",
        output_topic="+/some/subtopic",
    )
    assert r.to_output_topic("some/client_id/subtopic") == "client_id/some/subtopic"
    pass


def test_no_clientid_proxying():
    r = Retainer(
        input_topic="some/subtopic",
        output_topic="another/subtopic",
    )
    assert r.to_output_topic("some/subtopic") == "another/subtopic"
    pass


def test_drop_clientid():
    r = Retainer(
        input_topic="some/+/subtopic",
        output_topic="another/subtopic",
    )
    assert r.to_output_topic("some/cid/subtopic") == "another/subtopic"
    pass


def test_wildcard_only():
    r = Retainer(
        input_topic="any/#",
        output_topic="other/#",
    )
    assert r.to_output_topic("any/subtopic") == "other/subtopic"
    assert r.to_output_topic("any/sub/subtopic") == "other/sub/subtopic"
    pass


def test_clientid_and_wildcard():
    r = Retainer(
        input_topic="any/+/#",
        output_topic="other/+/foo/#",
    )
    assert r.to_output_topic("any/cid/subtopic") == "other/cid/foo/subtopic"
    assert r.to_output_topic("any/cid/sub/subtopic") == "other/cid/foo/sub/subtopic"
    pass
