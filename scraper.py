from bs4 import BeautifulSoup
from multiprocessing import Pool
from time import localtime, strftime
import urllib2
import re
import sys
import csv

# list of all player positions in the 2012 season
playerPositions = ['QUARTERBACK', 'RUNNING_BACK', 'WIDE_RECEIVER', 'TIGHT_END', 'DEFENSIVE_LINEMAN', 'LINEBACKER', 'DEFENSIVE_BACK', 'KICKOFF_KICKER', 'KICK_RETURNER', 'PUNTER', 'PUNT_RETURNER', 'FIELD_GOAL_KICKER']
#playerPositions = ['DEFENSIVE_BACK']
	
def run_scraper(pos):
	players = []
	colleges = []
	touchdowns = []
	yards = []

	# for when pages don't load
	reload_player_attempts = 1
	reload_career_attempts = 1

	# open, read, parse html source of posUrl
	posUrl = "http://www.nfl.com/stats/categorystats?tabSeq=1&season=2012&seasonType=REG&d-447263-p=1&statisticPositionCategory="+ pos
	posFile = urllib2.urlopen(posUrl)
	posHtml = posFile.read()
 	posFile.close()
	soup = BeautifulSoup("".join(posHtml), "lxml")

	pageLinks = soup.findAll(True, {"class": "linkNavigation"})

	# determine how many pages there are for each player position table
	if len(pageLinks) == 0:
		pageRange = range(1)
	elif len(pageLinks) > 0: 
		# get the number of table pages 
		pages = pageLinks[1].find_all("strong")
		pages = pages + pageLinks[1].find_all("a")
		pageRange = range(len(pages) - 1)

	for p in pageRange: # loop through all pages for player position table
		pageNum = p + 1 # current pageNum = 1, p starts at 0
		pageUrl = "http://www.nfl.com/stats/categorystats?tabSeq=1&season=2012&seasonType=REG"+"&d-447263-p="+ str(pageNum) +"&statisticPositionCategory="+ pos
		pageFile = urllib2.urlopen(pageUrl)
		pageHtml = pageFile.read()
		pageFile.close()
		soup = BeautifulSoup("".join(pageHtml), "lxml")
		table = soup.findAll(True, {"class" : re.compile(r"^(odd|even)$")})

		for t in table: # loop through all players on a single page
			row = t.find_all("td")
			player_name = row[1] .text.strip()
			players.append(player_name)

			

			playerHref = row[1].find("a") .get('href')
			playerHref = playerHref.replace("profile","careerstats") #direct to career stats 
			playerUrl = "http://www.nfl.com" + playerHref
			while(reload_player_attempts < 4): #player's career page sometimes have issues loading
				try:
					playerFile = urllib2.urlopen(playerUrl)
					break 
				except: # catch all exceptions
					e = sys.exc_info()[0]
					if reload_player_attempts == 1:
						open('player_page_errors.csv', 'w').close() #clear contents of player_page_errors.csv this is a new run
					efile = open('player_page_errors.csv', 'w') # write errors to player_page_errors.csv
					writer = csv.writer(efile)
					writer.writerow([playerUrl, "error number" + str(reload_player_attempts), e])
					reload_player_attempts += 1
			if reload_player_attempts > 3:
				continue	
			playerHtml = playerFile.read()
			playerFile.close()
			soup = BeautifulSoup("".join(playerHtml), "lxml")

			college = soup.find_all('strong', text = "College")[0].parent # get player's college
			colleges.append(college.text[9:]) 

			season = soup.find_all('strong', text = 'Experience')[0].parent #get the player's total number of seasons 
			season_str = season.text.split(" ")[1]
			if (season_str.endswith("th")) or (season_str.endswith("nd")) or (season_str.endswith("rd")) or (season_str.endswith("st")):
				season_str = season_str[:-2]

			careerTables = soup.find_all('div', id = 'player-stats-wrapper') #all tables on career stats page

			touchdowns.append(0) #initalize player's touchdowns to 0
			yards.append(0) #initalize player's touchdowns to 0
			for ct in careerTables[0].find_all('table'): #loop through all tables on career stats page
				headings = ct.find_all('thead')  #all stats headings of player 
				td_pos_counter = [] #holds the column number(s) in each table that contains the touchdowns
				yard_pos_counter = [] #holds the column number(s) in each table that contains the yards
				counter = 0 
				for h in headings[0].find_all('td'):
					if h.find_all(text = 'Team'):
						counter = 0
					if h.find_all(text = 'TD'):
						td_pos_counter.append(counter)
					if h.find_all(text = 'Yds'):
						yard_pos_counter.append(counter)
					counter += 1

				table_stats = ct.find_all('tbody')
				career_row = table_stats[0].find_all('tr', {"class" : "datatabledatahead"}) #player's career total stats for each table
				career_stats = career_row[0].find_all('td')
				for col in range(len(career_stats)):
					stat_string = career_stats[col].text
					if (stat_string.find('.') != -1) or (stat_string.find('-') != -1):
						continue # touchdowns and yards are never measured in decimals (source: football player friend)
					if col > 0:
						stat = int(stat_string.replace(',',''))
						if len(td_pos_counter) > 0:
							if (col == td_pos_counter[0]):
								touchdowns[len(touchdowns) - 1] += round(stat/float(season_str), 2)
								td_pos_counter.pop(0)
						if len(yard_pos_counter) > 0: 
							if (col == yard_pos_counter[0]):
								yards[len(touchdowns) - 1] += round(stat/float(season_str), 2)
								yard_pos_counter.pop(0)							
			
			print pos + " : " + player_name #prints player's name; use as feedback to keep track of threads

	college_dict = {}
	for i in range(len(colleges)):
		if colleges[i] in college_dict:
			college_dict[colleges[i]]["tds"] += touchdowns[i]
			college_dict[colleges[i]]["yards"] += yards[i]
			college_dict[colleges[i]]["players"] += 1
		else:
			college_dict[colleges[i]] = {}
			college_dict[colleges[i]]["tds"] = touchdowns[i]
			college_dict[colleges[i]]["yards"] = yards[i]
			college_dict[colleges[i]]["players"] = 1

	return college_dict

pool = []
if __name__ == '__main__':
	pool = Pool (processes = 12)
	result = pool.map(run_scraper, playerPositions)
	print result 

desired_stats = {}
for r in result:
	for k, v in r.iteritems():
		if k in desired_stats: 
			desired_stats[k]["tds"] += v["tds"]
			desired_stats[k]["yards"] += v["yards"]
			desired_stats[k]["players"] += v["players"]
		else: 
			desired_stats[k] = {}
			desired_stats[k]["tds"] = v["tds"]
			desired_stats[k]["yards"] = v["yards"]
			desired_stats[k]["players"] = v["players"]

writer = csv.writer(open('DesiredStats.csv', 'w')) # open DesiredStats.csv to write into
writer.writerow(["Colleges", "Total Players from College", "Average Touchdowns per Season of Players over their Entire Career from College", "Average Total Yards per Season of Players over their Entire Career from College", "Last updated:" + strftime("%Y-%m-%d %H:%M:%S", localtime())]) 
for k, v in desired_stats.iteritems(): 
	writer.writerow([k, v["players"], v["tds"], v["yards"]]) # write contents of desired_stats (dictionary) into rows of DesiredStats.csv 




