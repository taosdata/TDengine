import json
import logging

import pytest

from testng_taosx.explorer import Favorite, RestSQL
from testng_taosx.util import Util

favorite_test_logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def case_setup():
    favorite_test_logger.info("before all favorite cases...")
    env_data = Util.get_env_data()
    yield env_data
    favorite_test_logger.info("after all favorite cases...")


@pytest.mark.sanity
def test_sanity_favorite(case_setup):
    sql = "select * from test.`meters`"
    description = "test"
    description_edit = "edit test"
    favorite_test_logger.info("start favorite test...")
    env_data = case_setup

    favorite = Favorite(env_data)

    # create new favorite
    payload = {"sql": sql, "description": description}
    r = favorite.new_favorite(payload)
    favorite_test_logger.info(r.text)
    assert json.loads(r.text)["code"] == 0

    # query personal favorite list
    r = favorite.query_favorite(False, None)
    assert json.loads(r.text)["code"] == 0
    data = json.loads(r.text)["data"]
    assert data["list"][0]["sql"] == sql
    assert data["list"][0]["description"] == description
    assert data["list"][0]["is_public"] == False
    id = data["list"][0]["id"]
    total = data["total"]

    # create duplicate favorite
    r = favorite.new_favorite(payload)
    assert json.loads(r.text)["code"] == 102
    assert json.loads(r.text)["msg"] == "SQL already exists"

    # edit favorite description
    r = favorite.edit_favorite(id, description_edit)
    assert json.loads(r.text)["code"] == 0

    # share favorite
    r = favorite.share_favorite(id, True)
    assert json.loads(r.text)["code"] == 0

    # query public favorite list
    r = favorite.query_favorite(True, None)
    assert json.loads(r.text)["code"] == 0
    data = json.loads(r.text)["data"]
    assert data["list"][0]["sql"] == sql
    assert data["list"][0]["description"] == description_edit
    assert data["list"][0]["is_public"] is True
    assert data["list"][0]["id"] == id

    # unshare favorite
    r = favorite.share_favorite(id, False)
    assert json.loads(r.text)["code"] == 0

    # query public favorite list
    r = favorite.query_favorite(False, None)
    assert json.loads(r.text)["code"] == 0
    data = json.loads(r.text)["data"]
    assert data["list"][0]["sql"] == sql
    assert data["list"][0]["description"] == description_edit
    assert data["list"][0]["is_public"] is False
    assert data["list"][0]["id"] == id

    # delete exist favorite
    r = favorite.delete_favorite(id)
    assert json.loads(r.text)["code"] == 0

    # deleted not in list
    r = favorite.query_favorite(False, None)
    assert json.loads(r.text)["data"]["total"] == total - 1


@pytest.mark.parametrize(
    "tz_data",
    [
        ("", "+00:00"),
        # ("Europe/Rome", "+02:00"), # this timezone is not recommended
        ("Europe/Moscow", "+03:00"),
        ("Asia/Shanghai", "+08:00"),
        ("Asia/Tokyo", "+09:00"),
        ("America/Los_Angeles", "-07:00"),
    ],
)
@pytest.mark.sanity
def test_rest_api(case_setup, tz_data):
    rest_sql = RestSQL(case_setup)
    sql = "select * from information_schema.ins_dnodes limit 10"
    query_string = f"tz={tz_data[0]}"
    r = rest_sql.query_by_post(sql, query_string)
    assert json.loads(r.text)["code"] == 0
    assert tz_data[1] in json.loads(r.text)["data"][0][5]
