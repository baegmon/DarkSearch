import argparse
import json
import math
import requests
import threading
import time
import signal
import sys

from requests.exceptions import HTTPError

"""
DarkSearch.io API Disclaimer:
To avoid denial of service, our API is limited to 30 queries per minute.
If you exceed this quota, the API will send you a 429 error
PS: Excessing quotas too often can lead to banishment.
For any question or request, you can contact us at hello@darksearch.io.
"""

QUERIES_PER_MINUTE = 30
PROXY_FAIL_LIMIT = 10
NUMBER_OF_THREADS = 5

API_ENDPOINT = "https://darksearch.io/api/"

PROXIES = []

THREADS = []

RESULTS = []


class NoQueryException(Exception):
    pass


class QuotaOverloadException(Exception):
    pass


class InvalidPageException(Exception):
    pass


class Result:
    def __init__(self, title, link, description):
        self.title = title
        self.link = link
        self.description = description

def search(query, page=1, all=False):
    try:
        if query is None or len(query) <= 0:
            raise NoQueryException

        if page is None or page <= 0:
            raise InvalidPageException

        SEARCH_ENDPOINT = API_ENDPOINT + "search?query={}&page={}".format(query, page)
        response = requests.get(SEARCH_ENDPOINT)
        response.raise_for_status()

        if response.status_code is 429:
            raise QuotaOverloadException

        content = response.json()

        if content.get("total", None) is not None:
            total = content.get("total", None)
            current_page = content.get("current_page", 1)
            last_page = content.get("last_page", None)
            data = content.get("data", None)

            for r in data:
                result = Result(r.get("title"), r.get("link"), r.get("description"))

                RESULTS.append(result)

            print("Size of Results: {}".format(len(RESULTS)))
            print("Current Page: {}".format(current_page))
            print("Final Page: {}".format(last_page))

            remaining_pages = int(last_page) - int(current_page)
            queries_per_thread = math.ceil(remaining_pages / NUMBER_OF_THREADS)

            estimated_minutes = math.ceil(remaining_pages / QUERIES_PER_MINUTE)

            print("{} proxies have been added".format(len(PROXIES)))
            print(
                "Scraping will be performed using {} of threads.".format(
                    NUMBER_OF_THREADS
                )
            )
            print("There will be {} queries per thread.".format(queries_per_thread))

            # First page results have already been obtained.
            thread_current_page = int(current_page) + 1
            for i in range(0, NUMBER_OF_THREADS):
                thread_last_page = int(thread_current_page) + queries_per_thread

                if thread_last_page > last_page:
                    thread_last_page = last_page

                # def __init__(self, name, query, start, end):
                print(
                    "Thread {} Range: {} - {}".format(
                        i, thread_current_page, thread_last_page
                    )
                )
                search_thread = SearchThread(
                    i, query, thread_current_page, thread_last_page
                )
                search_thread.start()
                THREADS.append(search_thread)

                thread_current_page = thread_last_page + 1

            print(
                "Estimated Time: {} minutes.".format(
                    math.ceil(estimated_minutes / len(PROXIES))
                )
            )

    except NoQueryException as exception:
        print("Exception: No query specified. Exiting.")
    except InvalidPageException as exception:
        print("Exception: Page number must be greater than 1. Exiting.")
    except QuotaOverloadException as exception:
        print("API quota exceeded, please try again.")
    except HTTPError as exception:
        print("HTTP Error: {}".format(exception))
    except Exception as e:
        print("Exception: {}".format(e))


class SearchThread(threading.Thread):
    def __init__(self, name, query, start_page, end_page):
        threading.Thread.__init__(self)
        self.name = name
        self.kill_received = False
        self.query = query
        self.start_page = start_page
        self.end_page = end_page

    def run(self):
        counter = self.start_page

        # track the number of times a proxy fails consecutively
        proxy_fail_counter = 0
        proxy = PROXIES.pop()
        print("Thread {} is using proxy ({}).".format(self.name, proxy))

        # Two conditions for a thread to finish
        # 1. An abort signal is sent to the system
        # 2. The threads have retrieved all results from their assigned ranges
        while not self.kill_received and counter < self.end_page:

            # print("Kill Received: {}".format(self.kill_received))
            print(not self.kill_received or counter < self.end_page)

            try:
                if proxy_fail_counter == PROXY_FAIL_LIMIT:
                    proxy = PROXIES.pop()
                    print("Thread {} is using proxy ({}).".format(self.name, proxy))

                SEARCH_ENDPOINT = API_ENDPOINT + "search?query={}&page={}".format(
                    self.query, counter
                )
                response = requests.get(
                    SEARCH_ENDPOINT,
                    proxies={
                        "http": proxy,  # PROXIES[index],
                        "https": proxy,  # PROXIES[index]
                    },
                )
                response.raise_for_status()

                if response.status_code is 429:
                    raise QuotaOverloadException

                # print("Thread {}: Querying: {}".format(self.name, SEARCH_ENDPOINT))

                content = response.json()
                data = content.get("data", None)

                for r in data:
                    result = Result(r.get("title"), r.get("link"), r.get("description"))

                    RESULTS.append(result)

                print("Thread {}: Received {} results.".format(self.name, len(data)))
                counter = counter + 1
                # if successful, reset proxy fail counter to zero
                proxy_fail_counter = 0
            except InvalidPageException as exception:
                print("Exception: Page number must be greater than 1. Exiting.")
            except QuotaOverloadException as exception:
                print(
                    "Thread {}: API quota exceeded, please try again.".format(self.name)
                )
            except HTTPError as exception:
                print("Thread {}: HTTP Error: {}".format(self.name, exception))
                proxy_fail_counter = proxy_fail_counter + 1
            except Exception as e:
                print("Thread {}: Ran into an exception: {}".format(self.name, e))
                proxy_fail_counter = proxy_fail_counter + 1

            time.sleep(1)

        # when finished, add the proxy back to the list for use by other threads.
        PROXIES.append(proxy)

        print("Thread {}: has finished task execution.".format(self.name))


def export(filepath="results.json"):
    serialized = [obj.__dict__ for obj in RESULTS]

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"results": serialized}, f, ensure_ascii=False, indent=4)


def has_live_threads():
    return True in [thread.isAlive() for thread in THREADS]


def bye():
    sys.exit(0)


def main(args):
    if args.page is None:
        search(query=args.keyword, page=1)
    else:
        search(query=args.keyword, page=args.page)

    counter = 0

    while has_live_threads():
        if counter == 2:
            print("Counter is {}".format(counter))
            bye()

        try:
            # synchronization timeout of threads kill
            [t.join(1) for t in THREADS if t is not None and t.isAlive()]

        except KeyboardInterrupt:
            # Ctrl-C handling and send kill to threads
            print("Attempting to stop all threads gracefully...")
            for thread in THREADS:
                thread.kill_received = True
            counter = counter + 1

    if args.output is None:
        export()
    else:
        export(filepath=args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-k", "--keyword", help="Keyword(s) to search", required=True, type=str
    )
    parser.add_argument(
        "-o", "--output", help="Output the results into a file.", type=str
    )
    parser.add_argument(
        "-p", "--page", help="The page to parse the results from.", type=int
    )
    args = parser.parse_args()

    main(args)
