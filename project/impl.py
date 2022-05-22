from abc import ABC, abstractmethod
import pandas as pd


class RelationalProcessor():

    def __init__(self) -> None:
        self.dbPath = ""

    def getDbPath(self) -> str:
        return self.dbPath

    def setDbPath(self, path: str) -> None:
        self.dbPath = path


class RelationalDataProcessor(RelationalProcessor):

    # TODO: Implement upload Data
    def uploadData(self, path: str) -> None:
        pass


class TriplestoreProcessor():

    def __init__(self) -> None:
        self.endpointUrl = ""

    def getEndpointUrl(self) -> str:
        return self.endpointUrl

    def setEndpointUrl(self, url: str) -> None:
        self.endpointUrl = url


class TriplestoreDataProcessor(TriplestoreProcessor):

    # TODO: Implent Triplestore upload data
    def uploadData(self, path: str) -> None:
        pass


class QueryProcessor(ABC):

    @abstractmethod
    def getPublicationsPublishedInYear(self, year: int):
        pass

    @abstractmethod
    def getPublicationsByAuthorId(self, id: str):
        pass

    @abstractmethod
    def getMostCitedPublication(self):
        pass

    @abstractmethod
    def getMostCitedVenue(self):
        pass

    @abstractmethod
    def getVenuesByPublisherId(self, id: str):
        pass

    @abstractmethod
    def getPublicationInVenue(self, venueId: str):
        pass

    @abstractmethod
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str):
        pass

    @abstractmethod
    def getJournalArticlesInVolume(self, volume: str, journalId: str):
        pass

    @abstractmethod
    def getJournalArticlesInJournal(self, journalId: str):
        pass

    @abstractmethod
    def getProceedingsByEvent(self, eventPartialName: str):
        pass

    @abstractmethod
    def getPublicationAuthors(self, publicationId: str):
        pass

    @abstractmethod
    def getPublicationByAuthorName(self, authorPartialName: str):
        pass

    @abstractmethod
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)):
        pass


class RelationalQueryProcessor(QueryProcessor, RelationalProcessor):

    # TODO: Implement relational getPublicationsPublishedInYear
    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        pass

    # TODO: Implement relational getPublicationsByAuthorId
    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getMostCitedPublication
    def getMostCitedPublication(self) -> pd.DataFrame:
        pass

    # TODO: Implement relational getMostCitedVenue
    def getMostCitedVenue(self) -> pd.DataFrame:
        pass

    # TODO: Implement relational getVenuesByPublisherId
    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getPublicationInVenue
    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getJournalArticlesInIssue
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getJournalArticlesInVolume
    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getJournalArticlesInJournal
    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getProceedingsByEvent
    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getPublicationAuthors
    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getPublicationByAuthorName
    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getDistinctPublishersOfPublications
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)) -> pd.DataFrame:
        pass


class TripletoreQueryProcessor(QueryProcessor, TriplestoreProcessor):

    # TODO: Implement triplestore getPublicationsPublishedInYear
    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getPublicationsByAuthorId
    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getMostCitedPublication
    def getMostCitedPublication(self) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getMostCitedVenue
    def getMostCitedVenue(self) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getVenuesByPublisherId
    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getPublicationInVenue
    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getJournalArticlesInIssue
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getJournalArticlesInVolume
    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getJournalArticlesInJournal
    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getProceedingsByEvent
    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getPublicationAuthors
    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getPublicationByAuthorName
    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getDistinctPublishersOfPublications
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)) -> pd.DataFrame:
        pass


class GenericQueryProcessor(QueryProcessor):

    def __init__(self) -> None:
        self.queryProcessor = None

    def cleanQueryProcessors(self) -> None:
        pass

    def addQueryProcessor(self, processor: QueryProcessor) -> None:
        pass

    # TODO: Implement generic getPublicationsPublishedInYear
    def getPublicationsPublishedInYear(self, year: int):
        pass

    # TODO: Implement generic getPublicationsByAuthorId
    def getPublicationsByAuthorId(self, id: str):
        pass

    # TODO: Implement generic getMostCitedPublication
    def getMostCitedPublication(self):
        pass

    # TODO: Implement generic getMostCitedVenue
    def getMostCitedVenue(self):
        pass

    # TODO: Implement generic getVenuesByPublisherId
    def getVenuesByPublisherId(self, id: str):
        pass

    # TODO: Implement generic getPublicationInVenue
    def getPublicationInVenue(self, venueId: str):
        pass

    # TODO: Implement generic getJournalArticlesInIssue
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str):
        pass

    # TODO: Implement generic getJournalArticlesInVolume
    def getJournalArticlesInVolume(self, volume: str, journalId: str):
        pass

    # TODO: Implement generic getJournalArticlesInJournal
    def getJournalArticlesInJournal(self, journalId: str):
        pass

    # TODO: Implement generic getProceedingsByEvent
    def getProceedingsByEvent(self, eventPartialName: str):
        pass

    # TODO: Implement generic getPublicationAuthors
    def getPublicationAuthors(self, publicationId: str):
        pass

    # TODO: Implement generic getPublicationByAuthorName
    def getPublicationByAuthorName(self, authorPartialName: str):
        pass

    # TODO: Implement generic getDistinctPublishersOfPublications
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)):
        pass
