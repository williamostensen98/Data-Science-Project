class IdentifiableEntity():

    def __init__(self, id) -> None:
        self.id = id

    def getIds(self) -> str:
        return self.id


class Person(IdentifiableEntity):

    def __init__(self, givenName, familyName, id) -> None:
        super().__init__(id)
        self.givenName = givenName
        self.familyName = familyName

    def getGivenName(self) -> str:
        return self.givenName

    def getFamilyName(self) -> str:
        return self.familyName


class Organization(IdentifiableEntity):

    def __init__(self, id, name) -> None:
        super().__init__(id)
        self.name = name

    def getName(self) -> str:
        return self.name


class Venue(IdentifiableEntity):

    def __init__(self, id, title) -> None:
        super().__init__(id)
        self.title = title
        self.publisher = None

    def setPublisher(self, publisher: Organization) -> None:
        self.publisher = publisher

    def getTitle(self) -> str:
        return self.title

    def getPublisher(self) -> Organization or None:
        return self.publisher


class Publication(IdentifiableEntity):

    def __init__(self, id, publicationYear, title) -> None:
        super().__init__(id)
        self.publicationYear = publicationYear
        self.title = title
        self.cites = []
        self.publicationVenue = None
        self.authors = []

    def addCiting(self, publication) -> None:
        self.cites.append(publication)

    def addAuthors(self, author: Person) -> None:
        self.authors.append(author)

    def setPublicationVenue(self, venue):
        self.publicationVenue = venue

    def getPublicationYear(self) -> int or None:
        return self.publicationYear

    def getTitle(self) -> str:
        return self.title

    def getCitedPublications(self) -> list:
        return self.cites

    def getPublicationVenue(self) -> Venue or None:
        return self.publicationVenue

    def getAuthors(self) -> set[Person]:
        return set(self.authors)


class JournalArticle(Publication):

    def __init__(self, id, publicationYear, title, issue, volume) -> None:
        super().__init__(id, publicationYear, title)
        self.issue = issue
        self.volume = volume

    def getIssue(self) -> str:
        return self.issue

    def getVolume(self) -> str:
        return self.volume


class BookChapter(Publication):

    def __init__(self, id, publicationYear, title, chapterNumber) -> None:
        super().__init__(id, publicationYear, title)
        self.chapterNumber = chapterNumber

    def getChapterNumber(self) -> int:
        return self.chapterNumber


class ProceeedingsPaper(Publication):

    def __init__(self, id, publicationYear, title) -> None:
        super().__init__(id, publicationYear, title)


class Journal(Venue):

    def __init__(self, id, title) -> None:
        super().__init__(id, title)


class Book(Venue):

    def __init__(self, id, title) -> None:
        super().__init__(id, title)


class Proceedings(Venue):

    def __init__(self, id, title, event=None) -> None:
        super().__init__(id, title)
        self.event = event

    def getEvent(self) -> str:
        return self.event
