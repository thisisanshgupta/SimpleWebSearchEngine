from Crawler import crawler
from DataAccess import dataAccess

class searchEngine:
    def __init__(self, database):
        self.database = database

    def search(self, searchText):
        words = searchText.split(' ')
        n = len(words)
        searchQuery = 'select url.Link'
        selectQuery = [',u{}.Location'.format(i) for i in range(n)]
        wordLocationJoinQuery = [('UrlWordLocation u{}'.format(i), 'u{}'.format(i)) for i in range(n)]
        wordQuery = [('Word w{}'.format(i), 'w{}'.format(i)) for i in range(n)]

        for i in range(n):
            searchQuery += selectQuery[i]

        searchQuery += ' from '

        for i in range(len(words)):
            if i==0:
                searchQuery += wordLocationJoinQuery[i][0]
            else:
                searchQuery += ' inner join '
                searchQuery += wordLocationJoinQuery[i][0]
                searchQuery += ' on ' + wordLocationJoinQuery[i][1]+'.urlid = '+wordLocationJoinQuery[i-1][1]+'.urlid'

        for i in range(len(words)):
            columnMatch =  wordLocationJoinQuery[i][1]+'.WordId = '+wordQuery[i][1]+'.id'
            searchQuery += ' inner join '+wordQuery[i][0]
            searchQuery += ' on ' + columnMatch
        
        searchQuery += ' inner join url on u0.UrlId = url.id where '

        for i in range(len(words)):
            searchQuery += wordQuery[i][1]+'.word like \'' + words[i] +'\''
            if i != len(words) - 1:
                searchQuery += ' and '

        dataAcc = dataAccess(self.database)
        data = dataAcc.selectCommand(searchQuery)
        print(data)

    def crawlWebsite(self, domain):
        url = 'http://www.' + domain
        crawlerObj = crawler(self.database)
        crawlerObj.crawl(url, domain)

searchEng = searchEngine('D:\\SearchEngine.db')
#searchEng.crawlWebsite('practiceselenium.com')
searchEng.search('green tea')
