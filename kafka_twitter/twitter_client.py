import requests
import json


class ClsGetStream:

    def __init__(self, stream_url, token):
        self.stream_url = stream_url
        self.token = token
        self.payload = ""

    @property
    def headers(self):
        _headers = {
            "Authorization": "Bearer " + self.token
        }

        return _headers

    def get_data(self):

        response = requests.request("GET", self.stream_url, headers=self.headers, stream=True)

        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )

        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                yield json_response


if __name__ == '__main__':
    from config import *

    test_obj = ClsGetStream(SAMPLE_STREAM_URL, BEARER_TOKEN)

    test_obj.get_data()
