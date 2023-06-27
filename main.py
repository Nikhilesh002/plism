from collections import defaultdict
import os
import json
import requests
import re
import subprocess
from bs4 import BeautifulSoup
import csv 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
import aiohttp

CONTEST_SLUG = "turing-cup-2k23-round-1" # find in the contest URL
CHALLENGE_SLUGS = ["chain-beads"] # find in the challenge URL
CUTOFF_LIMIT = 100 # up to what rank should be in the plag report

# ==================================================================================================

# adding retry nd backoff to avoid Max retries exceeded with url / connection denied errors
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

contest_leaderboard_url = "https://www.hackerrank.com/rest/contests/{contest_slug}/leaderboard?limit={cutoff_limit}"
prblm_leaderboard_url = "https://www.hackerrank.com/rest/contests/{contest_slug}/challenges/{challenge_slug}/leaderboard?limit=1000&offset={offset}"
submission_url = "https://www.hackerrank.com/rest/contests/{contest_slug}/challenges/{challenge_slug}/hackers/{username}/download_solution"
challenges_url = "https://www.hackerrank.com/rest/contests/{contest_slug}/challenges"

agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}

def getLangKey(lang):
    # combine different versions
    langKey = re.sub(r"\d+$", "", lang)
    if langKey in ["c", "cpp"]:
        return "cc"    
    elif re.match("py*", langKey):
        langKey = "python"
    return langKey

async def download_and_write(session, url, filename):
    async with session.get(url, headers=agent) as response:
        with open(filename, "wb") as file:
            async for chunk in response.content.iter_chunked(1024):
                file.write(chunk)

async def saveSubmissionFiles(challenge, submissions):
    # saves files in diff dirs based on lang

    # due to connection pooling we're able to download 400 files within a min, instead of 15mins
    connector = aiohttp.TCPConnector(limit=500) 
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for lang in submissions:
            os.makedirs(challenge + "/" + lang, exist_ok=True)    
            usernames = submissions[lang]
            for username in usernames:
                filename = "{}/{}/{}".format(challenge, lang, username)
                url = submission_url.format(contest_slug=CONTEST_SLUG, challenge_slug=challenge, username=username)
                task = download_and_write(session, url, filename)
                tasks.append(task)
        await asyncio.gather(*tasks)


def getPrblmSubmissions(contest_slug, challenge_slug):
    submissions = defaultdict(lambda: [])
    offset = 0
    getMoreHackers = True

    while getMoreHackers:
        url = prblm_leaderboard_url.format(contest_slug=contest_slug, challenge_slug=challenge_slug, offset=offset)
        response = json.loads(session.get(url, headers=agent).content.decode('utf8'))
        leaderboard = response["models"]

        for hacker in leaderboard:
            username = hacker['hacker']
            score = hacker['score']
            lang = hacker['language']

            if score == 0:
                getMoreHackers = False
                break

            langKey = getLangKey(lang)
            submissions[langKey].append(username)

        offset += 20

    asyncio.run(saveSubmissionFiles(challenge, submissions))
    
    return submissions


def getTopHackers():
    # fetches the CUTOFF_LIMIT number of hackers from leaderboard
    url = contest_leaderboard_url.format(contest_slug=CONTEST_SLUG, cutoff_limit=CUTOFF_LIMIT)
    response = json.loads(session.get(url, headers=agent).content.decode('utf8'))["models"]
    hackers = [each["hacker"] for each in response]
    return hackers


# def runPlagCheckForTop(topHackers, submissions):
#     moss_urls = defaultdict(lambda: "") # username -- url
#     for lang in submissions:
#         usernames = submissions[lang]
#         for username in usernames:
#             if username in topHackers:
#                 # run moss
#                 subprocess.run("moss -l {lang} {lang}/{username} {lang}/*".format(lang=lang, username=username), shell=True) 
#                 print(username)

#     return moss_urls

async def runPlagCheckForAll(challenge, langs):
    moss_urls = ["http://moss.stanford.edu/results/3/7051883657436/"]
    # for lang in langs:
    #     result = subprocess.run("moss -l {lang} {challenge}/{lang}/*".format(challenge=challenge, lang=lang), shell=True, capture_output=True) 
    #     if result.stderr:
    #         print(result.stderr.decode('utf8'))
    #         exit(1)
        
    #     pattern = r'\n(.*?)\n$'
    #     match = re.search(pattern, result.stdout.decode('utf8'))
    #     if match:
    #         url = match.group(1)
    #         moss_urls.append(url)
    #         print(lang, url)
    connector = aiohttp.TCPConnector(limit=500) 
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in moss_urls:
            task = parseMoss(session, url)
            tasks.append(task)
        await asyncio.gather(*tasks)

hacker_url = defaultdict(lambda: "")
hacker_percentage = defaultdict(lambda: 0)

async def parseMoss(session, url):  
    url_pattern = r"http://moss\.stanford\.edu/results/\d+/\d+"
    if not re.match(url_pattern, url):
        return

    async with session.get(url) as response:
        html_content = await response.text()
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table")

        for row in table.find_all("tr"):
            for cell in row.find_all("td"):
                cell_text = cell.get_text()
                pattern = r"/(\w+) \((\d+)%\)"
                match = re.search(pattern, cell_text)
                if match:
                    hacker = match[1]
                    percentage = int(match[2])
                    cell_url = cell.a["href"]
                    if percentage > hacker_percentage[hacker]:
                        hacker_percentage[hacker] = percentage
                        hacker_url[hacker] = cell_url


def prepareResults():
    topHackers = getTopHackers()

    fields = ['Hacker', 'Max %', 'Corresp. Moss URL'] 
    rows = [[hacker, hacker_percentage[hacker], hacker_url[hacker]] for hacker in topHackers]
    filename = "plism_results.csv"

    with open(filename, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fields) 
        csvwriter.writerows(rows)


if __name__ == '__main__':
    challenges = CHALLENGE_SLUGS
    for challenge in challenges:
        print(challenge, "\n")

        os.makedirs(challenge, exist_ok=True)
        print("fetching submissions...")
        submissions = getPrblmSubmissions(CONTEST_SLUG, challenge)
        print("download of submissions complete...")
        
        print("running moss check...")
        asyncio.run(runPlagCheckForAll(challenge, submissions.keys()))
        print("moss check complete...")        
        print("=====================\n")
    
    # prepareResults()
    print("plism ended successfully!")




