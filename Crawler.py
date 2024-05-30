from urllib import request
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re
from DataAccess import dataAccess

class crawler:
    ignorewords = set(['a', 'an', 'the', 'in','is', 'am', 'are', 'was', 'were', 'will', 'shall', 'it', 'this', 'that', 'of', 'to', 'and'])

    #parameter database is database file path
    def __init__(self, database):
        self.database = database

    #Reads the page and return soup object of page content
    def getPage(self, url):
        try:
            httpOpen = request.urlopen(url, timeout = 10)
            content =  httpOpen.read()
            soup = BeautifulSoup(content)
            return soup
        except Exception as e:
            return None

    #Returns all page urls with url-text
    def getPageURLs(self, url, soup):
        links = soup('a')
        urls = []
        for link in links:
            if('href' in dict(link.attrs)):
                url = urljoin(url, link['href'])
                if url not in urls:
                    urls.append([url.split('#')[0], link.text])

        return urls

    #Returns array of word and word location
    def getWords(self, text):
        text = text.lower()
        words = re.compile(r'[^A-Z^a-z]+').split(text)
        filteredWords = []
        for i in range(len(words)):
            word = words[i]
            #Removing ignored words and blank spaces
            if word not in self.ignorewords and word != '':
                filteredWords.append((word, i))    #setting word location
        return filteredWords

    #Returns urlId if available in database or zero
    def getUrlId(self, url):
        dataAcc = dataAccess(self.database)
        data = dataAcc.selectCommand('SELECT Id FROM Url WHERE Link like \''+url+'\'')
        return  data[0][0] if len(data) > 0 else 0 

    #Insert new url
    def insertUrl(self, url):
         dataAcc = dataAccess(self.database)
         lastrowid = dataAcc.executeCommand('INSERT INTO Url(Link) VALUES(?)', (url,))
         return lastrowid

    #Get wordId from database
    def getWordId(self, word):
        dataAcc = dataAccess(self.database)
        wordData = dataAcc.selectCommand('SELECT Id, Word FROM Word WHERE Word like \''+word+'\'')
        wordId = 0
        if(len(wordData) > 0):      #if word is availabe in database what is id of that
            wordId = wordData[0][0]
        else:
            wordId = dataAcc.executeCommand('INSERT INTO Word(Word) VALUES(?)', (word,))
        return wordId

    #Insert new word
    def insertWordLocation(self, UrlId, word, word_location):
        wordId = self.getWordId(word)
        dataAcc = dataAccess(self.database)
        #Mapping of URL and Word and location
        dataAcc.executeCommand('INSERT INTO UrlWordLocation(UrlId, WordId, Location) VALUES(?,?,?)', (UrlId, wordId, word_location))

    #insert Link Words
    def insertLinkTextWord(self, urlId, word):
        wordId = self.getWordId(word)
        dataAcc = dataAccess(self.database)
        #Inser Link words
        dataAcc.executeCommand('INSERT INTO LinkWords(WordId, LinkId) VALUES(?,?)', (wordId, urlId))

    #insert FromUrl and ToUrl
    def insertFromToUrl(self, fromId, toId):
        dataAcc = dataAccess(self.database)
        lastrowid = dataAcc.executeCommand('INSERT INTO Link(FromId, ToId) VALUES(?,?)', (fromId,toId,))
        return lastrowid

    #index all pages in a website
    def crawl(self, url, domain, urlText = None, lastUrlId = None):
        urlId = self.getUrlId(url)
        #if url isn't indexed and in same domain
        if urlId == 0 and domain in url:
            soup = self.getPage(url)
            if(soup != None):
                print('indexing ', url)
                urls = self.getPageURLs(url, soup)
                words = self.getWords(soup.get_text())
                urlId = self.insertUrl(url)

                if urlText != None:
                    linkWords = self.getWords(urlText)
                    for word in linkWords:
                        if word not in self.ignorewords and word != '':
                            #mapping link with links in word
                            self.insertLinkTextWord(urlId, word[0])

                #inserting words location
                for word in words:
                    self.insertWordLocation(urlId, word[0], word[1])

                #recursive call to index all new urls found on this page
                for url in urls:
                    self.crawl(url[0], domain, url[1], urlId)

        #inserting from url, to url
        if lastUrlId != None and urlId != 0:
            self.insertFromToUrl(lastUrlId, urlId)