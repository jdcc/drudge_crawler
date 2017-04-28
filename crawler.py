import datetime, urllib.request, re, argparse
import bs4

BASE_URL = 'http://www.drudgereportarchives.com/'

INTERNAL_DATE_FORMAT = '%Y-%m-%d'

DEFAULT_START = datetime.datetime.now().strftime(INTERNAL_DATE_FORMAT)
DEFAULT_END = DEFAULT_START
DEFAULT_PER_DAY = 0

def parse_args():
    parser = argparse.ArgumentParser(description='This script is a small wrapper '
            'around the Drudrge Report Archives website that easily allows '
            'pulling the outbound links for a given date range into a CSV.')
    parser.add_argument('output', type=open, help='filename of the output CSV')
    parser.add_argument('--start', nargs='?', default=DEFAULT_START,
            help='first day to pull links down for, following the form "2017-01-13"')
    parser.add_argument('--end', nargs='?', default=DEFAULT_END,
            help='last day to pull links down for, following the form "2017-01-13"')
    parser.add_argument('--per_day', nargs='?', default=DEFAULT_PER_DAY,
            help='number of updates per day to parse - set to 0 for all')
    args = parser.parse_args()
    args.start = datetime.datetime.strptime(args.start, INTERNAL_DATE_FORMAT)
    args.end = datetime.datetime.strptime(args.end, INTERNAL_DATE_FORMAT)
    return args

class Timeline:
    FIRST_DATE = datetime.date(2001, 11, 18)
    URL = BASE_URL + 'dsp/timeline_html.htm'
    DAY_URL_TEMPLATE = BASE_URL + 'data/{year}/{month}/{day}/index.htm?s=flag'

    @classmethod
    def get_day_url(cls, day):
        cls._check_day_is_valid(day)
        return cls.format({
            'year': day.year(),
            'month': day.month(),
            'day': day.day()})

    @classmethod
    def _check_day_is_valid(cls, day):
        if day < FIRST_DATE or day > datetime.datetime.now():
            raise ArgumentError('Given date out of range: {}'.format(day))

class DayPage:
    def __init__(self, day):
        self.day = day
        self.url = Timeline.get_day_url(day)
        self.link_selector = 'a[href^="http://www.drudgereportArchives.com/data"]'

    def get_archives(self):
        f = urllib.request.urlopen(self.url)
        doc = bs4.BeautifulSoup(f)
        time_links = doc.select(self.link_selector)
        return [Archive(l['href']) for l in time_links]

    def get_n_archives(self, n):
        archives = self.get_archives()
        selected_archives = []
        for i in range(0, len(archives), interval):
            if len(selected_archives) < n:
                selected_archives.append(archives[i])
        return selected_archives

class Archive:
    def __init__(self, url):
        self.url = url
        self.datetime = self._url_to_datetime(url)
        pass

    def _url_to_datetime(self, url):
        match = re.search('([0-9_]*\).htm$', url)
        if match is None: return None
        date_format = '%Y%m%d_%H%M%S'
        return datetime.datetime.strptime(match.group(0), date_format)

    def links():
        f = urllib.request.urlopen(self.url)
        doc = bs4.BeautifulSoup(f)
        return [l['href'] for l in doc.select('a')]

class DrudgeCrawler:

    def __init__(self, start, end, per_day):
        self.start = start
        self.end = end
        self.per_day = per_day

    def get_links(self):
        for page in self.get_day_pages():


    def get_day_pages(self):
        return [DayPage(day) for day in _get_days_in_range(self.start, self.end)]

    def _get_days_in_range(self, start, end):
        days = []
        this_date = start.date()
        last_date = end.date()
        delta = datetime.timedelta(days=1)
        while this_date <= last_date:
            days.append(this_date)
            this_date += delta
        return days


