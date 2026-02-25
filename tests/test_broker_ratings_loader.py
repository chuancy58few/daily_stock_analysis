from src.broker_ratings import BrokerRatingsLoader


def test_broker_ratings_loader_basic(tmp_path):
    csv_path = tmp_path / "broker_ratings.csv"
    csv_path.write_text(
        "code,ms_stance,ms_as_of,ubs_stance,ubs_as_of,citi_stance,citi_as_of,updated_at,note\n"
        "600519,看多,2026-02-24,中性,2026-02-20,看空,2026-02-18,2026-02-24,example\n",
        encoding="utf-8",
    )
    loader = BrokerRatingsLoader(str(csv_path))
    data = loader.get_stances("600519")
    assert data["ms_stance"] == "看多"
    assert data["ubs_stance"] == "中性"
    assert data["citi_stance"] == "看空"


def test_broker_ratings_loader_missing_code(tmp_path):
    csv_path = tmp_path / "broker_ratings.csv"
    csv_path.write_text(
        "code,ms_stance,ms_as_of,ubs_stance,ubs_as_of,citi_stance,citi_as_of,updated_at,note\n",
        encoding="utf-8",
    )
    loader = BrokerRatingsLoader(str(csv_path))
    data = loader.get_stances("000001")
    assert data["ms_stance"] == "N/A"
    assert data["ubs_stance"] == "N/A"
    assert data["citi_stance"] == "N/A"
