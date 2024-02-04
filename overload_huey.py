from typing import Literal
import requests
import hueyqueue
import argparse
import sys

class GetPostRequest(object):
    def __init__(self, url = None):
        if url is None:
            raise ValueError("URL is required")
        self.url = url
        self._request = requests.session()
        

    def __call__(self, method: Literal['GET', 'POST'], *args, **kwargs):
        if method == 'GET':
            return self._request.get(self.url, *args, **kwargs)
        else:
            if kwargs.get('payload') is None:
                raise ValueError("payload: dict is required for POST request")
            return self._request.post(self.url, json=kwargs.get('payload'), **kwargs)
    

    def loop_request(self, method: Literal['GET', 'POST'], count: int, *args, **kwargs):
        for i in range(count):
            print(f"Executing {method} request to {self.url}, iteration {i + 1}")
            if method == 'GET':
                _res = self._request.get(self.url, *args, **kwargs)
            else:
                if kwargs.get('payload') is None:
                    raise ValueError("payload: dict is required for POST request")
                _res = self._request.post(self.url, json=kwargs.get('payload'), **kwargs)

            if _res.status_code < 200 or _res.status_code > 299:
                hueyqueue.logger.exception(f"Error in GET request: {self.url}")
                print(f"Error in GET request: {self.url}")
            else:
                print(f"Response: {_res.text}")
        return f"Executed {count} {method} requests to {self.url}"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='overload_huey.py',
                    description='Stress test hueyqueue',
                    epilog='Please break huey sqlite')
    parser.add_argument('-i', '--iterate', dest='count', type=int, default=10, help='number of request iterations')
    args = parser.parse_args()

    request_handler = GetPostRequest(url='http://127.0.0.1:5001/task')
    hueyqueue.logger.info("Executing GET request")
    request_handler.loop_request(method='GET', count=args.count)
    sys.exit(0)