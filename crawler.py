import datetime, urllib.request, re, argparse, csv, logging, math, concurrent.futures
import bs4, tqdm

MAX_WORKERS = 4

BASE_URL = 'http://www.drudgereportarchives.com/'

INTERNAL_DATE_FORMAT = '%Y-%m-%d'

DEFAULT_START = '2017-05-01'
DEFAULT_END = datetime.datetime.now().strftime(INTERNAL_DATE_FORMAT)
DEFAULT_PER_DAY = 0

def parse_args():
    parser = argparse.ArgumentParser(description='This script is a small wrapper '
            'around the Drudrge Report Archives website that easily allows '
            'pulling the outbound links for a given date range into a CSV.')
    parser.add_argument('output', help='filename of the output CSV')
    parser.add_argument('--start', nargs='?', default=DEFAULT_START,
            help='first day to pull links down for, following the form "2017-01-13"')
    parser.add_argument('--end', nargs='?', default=DEFAULT_END,
            help='last day to pull links down for, following the form "2017-01-13"')
    parser.add_argument('--per_day', nargs='?', type=int, default=DEFAULT_PER_DAY,
            help='number of updates per day to parse - set to 0 for all')
    args = parser.parse_args()
    args.start = datetime.datetime.strptime(args.start, INTERNAL_DATE_FORMAT)
    args.end = datetime.datetime.strptime(args.end, INTERNAL_DATE_FORMAT)
    return args

class Timeline:
    FIRST_DATE = datetime.date(2001, 11, 18)
    URL = BASE_URL + 'dsp/timeline_html.htm'
    DAY_URL_TEMPLATE = BASE_URL + 'data/{year}/{month:02}/{day:02}/index.htm?s=flag'

    @classmethod
    def get_day_url(cls, day):
        cls._check_day_is_valid(day)
        return cls.DAY_URL_TEMPLATE.format(
            year=day.year,
            month=day.month,
            day=day.day)

    @classmethod
    def _check_day_is_valid(cls, day):
        if day < cls.FIRST_DATE or day > datetime.date.today():
            raise ArgumentError('Given date out of range: {}'.format(day))

class DayPage:
    def __init__(self, day):
        self.day = day
        self.url = Timeline.get_day_url(day)
        self.link_selector = 'a[href^="http://www.drudgereportArchives.com/data"]'

    def get_archives(self):
        logging.debug('Fetching URL: {}'.format(self.url))
        try:
            f = urllib.request.urlopen(self.url)
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError) as e:
            logging.error(e)
            return []

        doc = bs4.BeautifulSoup(f, 'lxml')
        time_links = doc.select(self.link_selector)
        return [Archive(l['href']) for l in time_links]

    def get_n_archives(self, n):
        archives = self.get_archives()
        if len(archives) == 0:
            return []
        selected_archives = []
        interval = math.floor(len(archives) / n)
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
        match = re.search('([0-9_]*).htm$', url)
        if match is None: return None
        date_format = '%Y%m%d_%H%M%S'
        return datetime.datetime.strptime(match.group(1), date_format)

    def get_links(self):
        logging.debug('Fetching URL: {}'.format(self.url))
        try:
            f = urllib.request.urlopen(self.url)
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError) as e:
            logging.error(e)
            return []

        doc = bs4.BeautifulSoup(f, 'lxml')
        rules = doc.select('hr[color="#0000A0"]')
        if len(rules) > 0:
            links = doc.select('hr[color="#0000A0"]')[1].find_all_next('a')
        elif len(rules) > 2:
            footer_links = doc.select('hr[color="#0000A0"]')[2].find_all_next('a')
            links = links - footer_links
        else:
            links = doc.select('a')

        return [l['href'].strip() for l in links]

class DrudgeCrawler:

    def __init__(self, start, end, per_day):
        self.start = start
        self.end = end
        self.per_day = per_day

    def get_archives(self):
        archives = []
        for page in self.get_day_pages():
            if self.per_day > 0:
                day_archives = page.get_n_archives(self.per_day)
            else:
                day_archives = page.get_archives()
            archives += day_archives
        return archives

    def get_day_pages(self):
        return [DayPage(day) for day in self._get_days_in_range(self.start, self.end)]

    def _get_days_in_range(self, start, end):
        days = []
        this_date = start.date()
        last_date = end.date()
        delta = datetime.timedelta(days=1)
        while this_date <= last_date:
            days.append(this_date)
            this_date += delta
        return days

def run(start, end, per_day, output):
    crawler = DrudgeCrawler(start, end, per_day)
    archives = crawler.get_archives()
    first_appearances = {}
    with open(output, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['datetime', 'link_url', 'archive_url'])
        writer.writeheader()
        future_to_archive = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor, tqdm.tqdm(total=len(archives), smoothing=0) as pbar:
            for archive in archives:
                future_to_archive[executor.submit(archive.get_links)] = archive
            for future in concurrent.futures.as_completed(future_to_archive.keys()):
                archive = future_to_archive[future]
                try:
                    links = future.result()
                except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError) as e:
                    logging.error(e)
                    continue
                for link in links:
                    if link not in first_appearances or archive.datetime < first_appearances[link]:
                        first_appearances[link] = archive.datetime
                    writer.writerow({
                        'datetime': archive.datetime,
                        'link_url': link,
                        'archive_url': archive.url})
                pbar.update()

    with open('first_appearances.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'first_appearance'])
        writer.writeheader()
        for url, time in first_appearances.items():
            writer.writerow({ 'url': url, 'first_appearance': time })

if __name__ == '__main__':
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    run(args.start, args.end, args.per_day, args.output)
