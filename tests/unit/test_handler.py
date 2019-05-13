import json

from toll_booth import app


def test_lambda_handler(api_gw_event):

    ret = app.lambda_handler(api_gw_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "hello world, i am algernon"
