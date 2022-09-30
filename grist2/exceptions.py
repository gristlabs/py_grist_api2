import pprint


class APIError(Exception):
    def __init__(self, url, response, response_json=None, message=None):
        self.url = url
        self.response = response
        self.response_json = response_json
        self._message = message

    def __str__(self):
        error = self._message or pprint.pformat(self.response_json)
        return f'Error at {self.url}, code {self.response.status_code}:\n{error}'
