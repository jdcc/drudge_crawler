import csv, collections, datetime
import tldextract

def process_first_appearances():
    domains = []
    with open('first_appearances.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ext = tldextract.extract(row['url'])
            domain = '.'.join(part for part in ext if part and part != 'www')
            domains.append(domain)

    with open('domain_counts.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['domain', 'count'])
        writer.writeheader()
        for domain, count in collections.Counter(domains).items():
            writer.writerow({ 'domain': domain, 'count': count })

def domains_by_month():
    domains_by_month = collections.defaultdict(collections.Counter)
    with open('all_links.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ext = tldextract.extract(row['link_url'])
            domain = '.'.join(part for part in ext if part and part != 'www')
            date = datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            month_key = '{}-{}'.format(date.year, date.month)
            domains_by_month[month_key][domain] += 1

    with open('domain_counts_by_month.csv', 'w') as f, open('num_domains_by_month.csv', 'w') as g:
        writer = csv.DictWriter(f, fieldnames=['month', 'domain', 'count'])
        num_writer = csv.DictWriter(g, fieldnames=['month', 'count'])
        writer.writeheader()
        num_writer.writeheader()
        for month, domains in domains_by_month.items():
            num_writer.writerow({ 'month': month, 'count': len(domains) })
            for domain, count in domains.items():
                writer.writerow({ 'month': month, 'domain': domain, 'count': count })
domains_by_month()
