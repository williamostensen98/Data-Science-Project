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


class RelationalQueryProcessor(QueryProcessor):

    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        pass
