from alphapulse.sources.bilibili.ids import parse_space_mid, parse_video_target


def test_parse_video_target_from_canonical_url() -> None:
    parsed = parse_video_target("https://www.bilibili.com/video/BV1xx411c7mu", "https://www.bilibili.com")
    assert parsed is not None
    assert parsed.bvid == "BV1xx411c7mu"
    assert parsed.canonical_url == "https://www.bilibili.com/video/BV1xx411c7mu"


def test_parse_video_target_from_bvid() -> None:
    parsed = parse_video_target("BV1xx411c7mu", "https://www.bilibili.com")
    assert parsed is not None
    assert parsed.bvid == "BV1xx411c7mu"


def test_parse_video_target_from_avid() -> None:
    parsed = parse_video_target("av123456", "https://www.bilibili.com")
    assert parsed is not None
    assert parsed.aid == 123456
    assert parsed.canonical_url == "https://www.bilibili.com/video/av123456"


def test_parse_video_target_rejects_invalid_input() -> None:
    assert parse_video_target("not-a-video", "https://www.bilibili.com") is None


def test_parse_space_mid_from_url() -> None:
    assert parse_space_mid("https://space.bilibili.com/7033507") == "7033507"
    assert parse_space_mid("https://space.bilibili.com/7033507/video") == "7033507"
    assert parse_space_mid("  https://space.bilibili.com/7033507?foo=bar  ") == "7033507"


def test_parse_space_mid_from_bare_mid() -> None:
    assert parse_space_mid("7033507") == "7033507"


def test_parse_space_mid_rejects_invalid_input() -> None:
    assert parse_space_mid("") is None
    assert parse_space_mid("not-a-space") is None
    assert parse_space_mid("https://www.bilibili.com/video/BV1xx411c7mu") is None
