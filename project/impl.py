from abc import ABC, abstractmethod
import pandas as pd
from rdflib import Graph
from rdflib import URIRef
from rdflib import Literal
from rdflib import RDF
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
import json
from sqlite3 import connect
from models import *


class Schema():

    # classes of resources
    JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
    BookChapter = URIRef("https://schema.org/Chapter")
    Journal = URIRef("https://schema.org/Periodical")
    Book = URIRef("https://schema.org/Book")
    Publisher = URIRef("https://schema.org/Organization")
    Proceedings = URIRef("https://schema.org/Event")
    ProceedingsPaper = URIRef("https://schema.org/Report")
    Person = URIRef("https://schema.org/Person")

    # attributes related to classes
    doi = URIRef("https://schema.org/identifier")
    publicationYear = URIRef("https://schema.org/datePublished")
    title = URIRef("https://schema.org/name")
    issue = URIRef("https://schema.org/issueNumber")
    chapter = URIRef("https://schema.org/bookEdition")
    volume = URIRef("https://schema.org/volumeNumber")
    identifier = URIRef("https://schema.org/identifier")
    name = URIRef("https://schema.org/name")
    event = URIRef("https://schema.org/description")
    familyName = URIRef("https://schema.org/familyName")
    givenName = URIRef("https://schema.org/givenName")

    # relations among classes
    publicationVenue = URIRef("https://schema.org/isPartOf")
    citation = URIRef("https://schema.org/citation")
    publisher = URIRef("https://schema.org/isPartOf")
    author = URIRef("https://schema.org/author")


class RelationalProcessor():

    def __init__(self) -> None:
        self.dbPath = ""

    def getDbPath(self) -> str:
        return self.dbPath

    def setDbPath(self, path: str) -> None:
        self.dbPath = path
        
class RelationalProcessor():

    def __init__(self) -> None:
        self.dbPath = ""

    def getDbPath(self) -> str:
        return self.dbPath

    def setDbPath(self, path: str) -> None:
        self.dbPath = path


class RelationalDataProcessor(RelationalProcessor):

    def __init__(self) -> None:
        super().__init__()

    def add_internalID(self, df, id_name):
        internal_id = []
        for idx, row in df.iterrows():
            internal_id.append(id_name + "-" + str(idx))

        df.insert(0, id_name + "_internalID",
                  pd.Series(internal_id, dtype="string"))

        return df

    def uploadData(self, path: str) -> None:
        db_path = self.getDbPath()

        with connect(db_path) as con:

            con.commit()

        file_ending = path.split(".")
        file_type = file_ending[1]

        if file_type == 'json':

            # DATAFRAME AUTHORS & AUTHOR_LIST
            with open(path, "r", encoding="utf-8") as f:
                json_doc = json.load(f)
                doi_authors = json_doc["authors"]

                df_author = pd.DataFrame(
                    {'family': [], 'given': [], 'orcid': []})
                df_doi_orcid = pd.DataFrame({'doi': [], 'orcid': []})

                for doi in doi_authors:

                    for author in doi_authors[doi]:

                        new_row_author = pd.DataFrame(author, index=[0])
                        df_author = pd.concat(
                            [new_row_author, df_author.loc[:]]).reset_index(drop=True)

                        dict = {'doi': doi, 'orcid': author['orcid']}
                        new_row_orcid = pd.DataFrame(dict, index=[0])
                        df_doi_orcid = pd.concat(
                            [new_row_orcid, df_doi_orcid.loc[:]]).reset_index(drop=True)

                df_author = df_author.drop_duplicates(
                    subset=['orcid'], keep='last').reset_index(drop=True)

            # DATAFRAME PUBLISHERS

                publishers = json_doc["publishers"]

                l_name = []
                l_crossref = []

                for ref in publishers:  # make 2-col DataFrame; doi, dict authours
                    l_name.append(publishers[ref]["name"])
                    l_crossref.append(publishers[ref]['id'])

                df_organisation = pd.DataFrame(
                    {'name': pd.Series(l_name), 'crossref': pd.Series(l_crossref)})

                #df_organisation = self.add_internalID(df, 'organisation')

            # DATAFRAME ISSN

                issn = json_doc["venues_id"]

                l_doi = []
                l_issn = []

                for key in issn:

                    for i in issn[key]:
                        l_issn.append(i)
                        l_doi.append(key)

                df_issn = pd.DataFrame(
                    {"issn": pd.Series(l_issn), "doi": pd.Series(l_doi)})

            # DATAFRAME REFERENCES

                references = json_doc["references"]

                l_doi = []
                l_references = []

                for key in references:

                    for i in references[key]:
                        l_doi.append(i)
                        l_references.append(key)

                df_references = pd.DataFrame(
                    {"doi": pd.Series(l_doi), "referes_to": pd.Series(l_references)})

            # ADD JSON-DATAFRAMES TO DATABASE
            with connect(db_path) as con:

                df_organisation.to_sql(
                    "organisations", con, if_exists="replace", index=False)
                df_author.to_sql(
                    "authors", con, if_exists="replace", index=False)
                df_doi_orcid.to_sql("authorslist", con,
                                    if_exists='replace', index=False)
                df_references.to_sql("references_to", con,
                                     if_exists='replace', index=False)
                df_issn.to_sql("issn", con, if_exists='replace', index=False)

        if file_type == 'csv':

            # DATAFRAME VENUES
            df_csv = pd.read_csv(path,
                                 keep_default_na=False,
                                 dtype={
                                     "id": "string",
                                     "title": "string",
                                     "type": "string",
                                     "publication_year": "int",
                                     "issue": "string",
                                     "volume": "string",
                                     "chapter": "string",
                                     "publication_venue": "string",
                                     "venue_type": "string",
                                     "publisher": "string",
                                     "event": "string"
                                 })
            
            df_csv =  df_csv.rename(columns={"publication_year": "year", "publication_venue": "venueName", "venue_type": "venueType"})
            venues = df_csv[['venueName', 'venueType', 'event']]
            venues = self.add_internalID(venues, 'venue')
            
            venues_distinct = venues.drop_duplicates(
                subset=None, keep='first', inplace=False, ignore_index=False)
            venues = venues_distinct.reset_index(drop=True)

            df_venues = venues
            
            # DATAFRAME PUBLICATIONS
            publication_internal_id = []
            for idx, row in df_csv.iterrows():
                publication_internal_id.append("publication-" + str(idx))
            df_csv.insert(0, "internalId", pd.Series(
                publication_internal_id, dtype="string"))

            
            df_publications_merge = pd.merge(df_csv, df_venues[['venue_internalID', 'venueName']], on='venueName')
            df_publications =  df_publications_merge.rename(columns={"venue_internalID": "venueId"})
            
            # ADD CSV-DATAFRAMES TO DATABASE
            with connect(db_path) as con:
                df_publications.to_sql(
                    "publications", con, if_exists='replace', index=False)
                df_venues.to_sql(
                    "venues", con, if_exists='replace', index=False)

        elif file_type != 'json' and file_type != 'csv':
            print("FILE TYPE NOT SUPPORTED")
            return

       # print(df_publications.columns, df_venues.columns, df_author.columns, df_organisation.columns, df_doi_orcid.columns, df_issn.columns, df_references)
class TriplestoreProcessor():

    # This is the string defining the base URL used to defined
    # the URLs of all the resources created from the data
    base_url = "https://comp-data.github.io/res/"
    graph = Graph()
    store = SPARQLUpdateStore()

    def __init__(self) -> None:
        self.endpointUrl = ""

    def getEndpointUrl(self) -> str:
        return self.endpointUrl

    def setEndpointUrl(self, url: str) -> None:
        self.endpointUrl = url


class TriplestoreDataProcessor(TriplestoreProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.publication_internal_id = {}  # id: subj
        self.venues_internal_id = {}  # doi: subj
        self.publisher_internal_id = {}  # id: subj
        self.author_internal_id = {}  # id: subj
        self.pub_idx = 0
        self.ven_idx = 0
        self.auth_idx = 0
        self.org_idx = 0

    def uploadData(self, path: str) -> None:
        if self.get_file_ending(path) == "json":
            try:
                self.handle_json_upload(path)
            except OSError:
                raise RuntimeError from None
        else:
            try:
                self.handle_csv_upload(path)
            except OSError:
                raise RuntimeError from None

        # It opens the connection with the SPARQL endpoint instance
        TriplestoreProcessor.store.open(
            (self.getEndpointUrl(), self.getEndpointUrl()))

        for triple in TriplestoreProcessor.graph.triples((None, None, None)):
            TriplestoreProcessor.store.add(triple)

        TriplestoreProcessor.store.close()
        print("GRAPH DATA UPLOADED")

    def handle_csv_upload(self, path: str) -> None:
        publications = pd.read_csv(path,
                                   keep_default_na=False,
                                   dtype={
                                       "id": "string",
                                       "title": "string",
                                       "publication_year": "int",
                                       "publication_venue": "string",
                                       "type": "string",
                                       "issue": "string",
                                       "volume": "string",
                                       "chapter": "string",
                                       "venue_type": "string",
                                       "publisher": "string",
                                       "event": "string"
                                   })
        venue_internal_name = {}
        for idx, row in publications.iterrows():

            if row['id'] in self.publication_internal_id:
                subject = self.publication_internal_id[row['id']]
                # Means that it has been added by json file, so we just get the pointers
            else:
                local_id = "publication" + str(self.pub_idx)
                self.pub_idx += 1
                subject = URIRef(TriplestoreProcessor.base_url + local_id)
                self.publication_internal_id[row['id']] = subject
                TriplestoreProcessor.graph.add(
                    (subject, Schema.identifier, Literal(row["id"])))

            # Create Venue Object unless it has already been set
            if row["publication_venue"] in venue_internal_name:
                venue = self.venues_internal_id[venue_internal_name[row["publication_venue"]]]
            elif row['id'] in self.venues_internal_id:
                venue = self.venues_internal_id[row['id']]
            else:
                venue_id = "venue-" + str(self.ven_idx)
                self.ven_idx += 1
                venue = URIRef(TriplestoreProcessor.base_url + venue_id)
                self.venues_internal_id[row['id']] = venue
                venue_internal_name[row["publication_venue"]] = row['id']

            # Create publisher object
            if row['publisher'] in self.publisher_internal_id:
                publisher = self.publisher_internal_id[row['publisher']]
            else:
                org_id = "publisher-" + str(self.org_idx)
                self.org_idx += 1
                publisher = URIRef(TriplestoreProcessor.base_url + org_id)
                self.publisher_internal_id[row['publisher']] = publisher

            # ADD VENUE DATA - name and type
            ven_name = row["publication_venue"]
            ven_type = row["venue_type"]

            if ven_name not in venue_internal_name:
                venue_internal_name[ven_name] = row['id']
            TriplestoreProcessor.graph.add(
                (venue, Schema.name, Literal(ven_name)))

            if ven_type == "journal":
                TriplestoreProcessor.graph.add(
                    (venue, RDF.type, Schema.Journal))
            elif ven_type == "book":
                TriplestoreProcessor.graph.add(
                    (venue, RDF.type, Schema.Book))
            else:
                TriplestoreProcessor.graph.add(
                    (venue, RDF.type, Schema.Proceedings))

                TriplestoreProcessor.graph.add(
                    (venue, Schema.event, Literal(row['event'])))

            # Add relationship between publication and venue
            TriplestoreProcessor.graph.add(
                (subject, Schema.publicationVenue, venue))

            # Add relationship between venue and publisher
            TriplestoreProcessor.graph.add(
                (venue, Schema.publisher, publisher))

            # ADD PUBLICATION DATA
            if row["type"] == "journal-article":
                TriplestoreProcessor.graph.add(
                    (subject, RDF.type, Schema.JournalArticle))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.issue, Literal(row["issue"])))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.volume, Literal(row["volume"])))
            else:
                TriplestoreProcessor.graph.add(
                    (subject, RDF.type, Schema.BookChapter))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.chapter, Literal(row["chapter"])))

            TriplestoreProcessor.graph.add(
                (subject, Schema.name, Literal(row["title"])))
            TriplestoreProcessor.graph.add(
                (subject, Schema.publicationYear, Literal(str(row["publication_year"]))))

    def handle_json_upload(self, path: str) -> None:
        with open(path) as f:
            d = json.load(f)

        authors = pd.json_normalize(d['authors'])
        venues = pd.json_normalize(d['venues_id'])
        publishers = d['publishers']
        citations = pd.json_normalize(d['references'])

        # Adding authors to publications
        self.addAuthors(authors)

        # Adding venues to publications
        self.addVenues(venues)
        self.addPublishers(publishers)
        self.addReferences(citations)

    def get_file_ending(self, filename: str) -> str:
        ending = filename.split(".")
        return ending[1]

    def addAuthors(self, authors):
        for doi in authors:
            for author in authors[doi][0]:
                local_id = "author-" + str(self.auth_idx)
                self.auth_idx += 1
                subject = URIRef(TriplestoreProcessor.base_url + local_id)
                if doi not in self.author_internal_id:
                    self.author_internal_id[doi] = [subject]
                else:
                    self.author_internal_id[doi].append(subject)
                TriplestoreProcessor.graph.add(
                    (subject, RDF.type, Schema.Person))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.identifier, Literal(author['orcid'])))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.givenName, Literal(author['given'])))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.familyName, Literal(author['family'])))

                # If the publications is not already in the graph we add it along with only the
                # identifier. Then we add the relationship of the author
                if doi not in self.publication_internal_id:
                    pub_id = "publication" + str(self.pub_idx)
                    self.pub_idx += 1
                    sub = URIRef(TriplestoreProcessor.base_url + pub_id)
                    self.publication_internal_id[doi] = sub
                    TriplestoreProcessor.graph.add(
                        (sub, Schema.identifier, Literal(doi)))

                TriplestoreProcessor.graph.add(
                    (self.publication_internal_id[doi], Schema.author, subject))

    def addVenues(self, venues):

        for doi in venues:

            # Means that venue has not been created before
            if doi not in self.publication_internal_id:
                # Create publication object
                pub_id = "publication" + str(self.pub_idx)
                self.pub_idx += 1
                publication = URIRef(TriplestoreProcessor.base_url + pub_id)
                self.publication_internal_id[doi] = publication
                TriplestoreProcessor.graph.add(
                    (publication, Schema.identifier, Literal(doi)))
            else:
                publication = self.publication_internal_id[doi]

            # Add identifiers
            venue_publication = {}
            for id in venues[doi][0]:
                if id in venue_publication:

                    subject = self.venues_internal_id[self.venue_publication[id]]

                elif doi in self.venues_internal_id:

                    subject = self.venues_internal_id[doi]
                else:
                    # Create Venue Object
                    local_id = "venue-" + str(self.ven_idx)
                    self.ven_idx += 1
                    subject = URIRef(TriplestoreProcessor.base_url + local_id)
                    self.venues_internal_id[doi] = subject
                    venue_publication[id] = doi

                # Add relationship between publication and venue
                TriplestoreProcessor.graph.add(
                    (publication, Schema.publicationVenue, subject))

                TriplestoreProcessor.graph.add(
                    (subject, Schema.identifier, Literal(id)))

    def addPublishers(self, publishers):
        for crossref in publishers.items():
            if crossref[1]['id'] in self.publisher_internal_id:
                publisher = self.publisher_internal_id[crossref[1]['id']]
            else:
                org_id = "publisher-" + str(self.org_idx)
                self.org_idx += 1
                publisher = URIRef(TriplestoreProcessor.base_url + org_id)
                self.publisher_internal_id[crossref[1]['id']] = publisher
            TriplestoreProcessor.graph.add(
                (publisher, Schema.identifier, Literal(crossref[1]['id'])))
            TriplestoreProcessor.graph.add(
                (publisher, Schema.name, Literal(crossref[1]['name'])))

    def addReferences(self, citations):
        for doi in citations:
            if doi in self.publication_internal_id:
                publication = self.publication_internal_id[doi]
            else:
                pub_id = "publication" + str(self.pub_idx)
                self.pub_idx += 1
                publication = URIRef(TriplestoreProcessor.base_url + pub_id)
                self.publication_internal_id[doi] = publication
                TriplestoreProcessor.graph.add(
                    (publication, Schema.identifier, Literal(doi)))

            for reference in citations[doi][0]:
                if reference in self.publication_internal_id:
                    cite = self.publication_internal_id[reference]
                else:
                    pub_id = "publication" + str(self.pub_idx)
                    self.pub_idx += 1
                    cite = URIRef(TriplestoreProcessor.base_url + pub_id)
                    self.publication_internal_id[doi] = cite
                    TriplestoreProcessor.graph.add(
                        (cite, Schema.identifier, Literal(reference)))
                TriplestoreProcessor.graph.add(
                    (publication, Schema.citation, cite))


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
    def getDistinctPublishersOfPublications(self, pubIdList):
        pass


class RelationalQueryProcessor(QueryProcessor, RelationalProcessor):

    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT * FROM publications WHERE year = %d" %year
            df_sql = pd.read_sql(query, con)
                 
        return df_sql

    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT doi FROM authorslist WHERE authororcID = '%s'" % id
            df_sql = pd.read_sql(query, con)

        return df_sql

    def getMostCitedPublication(self) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT referes_to, COUNT(*) FROM references_to GROUP BY referes_to"
            df_sql = pd.read_sql(query, con)

            publication = "SELECT * FROM publications"
            df_publications = pd.read_sql(publication, con)

            max = df_sql.loc[df_sql['COUNT(*)'].idxmax()]
            max = max['referes_to']

            df_max = pd.DataFrame({"doi": pd.Series(max)})

            result = pd.merge(df_publications, df_max,
                              left_on='id', right_on='doi')
        return result

    def getMostCitedVenue(self) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT publication_venue, id FROM publications"
            df_sql = pd.read_sql(query, con)

            query2 = "SELECT referes_to, COUNT(*) FROM references_to GROUP BY referes_to"
            df_sql2 = pd.read_sql(query2, con)

            df_result = pd.merge(
                df_sql, df_sql2, left_on='id', right_on='referes_to')
            df_result = df_result.groupby(
                by=["publication_venue"]).sum().reset_index()

            max = df_result.loc[df_result['COUNT(*)'].idxmax()]
            result_venue = max['publication_venue']

            query3 = "SELECT * FROM venues WHERE publication_venue = '%s'" % result_venue
            df_sql3 = pd.read_sql(query3, con)

        return df_sql3

    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT publication_venue FROM publications WHERE crossref = '%s'" % id
            df_sql = pd.read_sql(query, con)
            df_sql1 = df_sql.drop_duplicates(
                subset=None, keep='first', inplace=False, ignore_index=False)

        return df_sql1

    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT crossref FROM venues WHERE issn = '%s'" % venueId
            df_sql = pd.read_sql(query, con)

            df_empty = pd.DataFrame()
            for crossref in df_sql['crossref']:

                query2 = "SELECT * FROM publications WHERE crossref = '%s'" % crossref
                df_publication = pd.read_sql(query2, con)

                df_empty = pd.concat(
                    [df_empty, df_publication], ignore_index=True)

            df_result = df_empty.drop_duplicates(
                subset=None, keep='first', inplace=False, ignore_index=False)

        return df_sql

    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        with connect("relational.db") as con:
            publication = "SELECT * FROM publications"
            df_publications = pd.read_sql(publication, con)

            query_issue = "SELECT id FROM publications WHERE issue = '%s'" % issue
            issue_sql = pd.read_sql(query_issue, con)

            query_volume = "SELECT id FROM publications WHERE volume = '%s'" % volume
            volume_sql = pd.read_sql(query_volume, con)

            query = "SELECT doi FROM issn WHERE issn = '%s'" % journalId
            df_sql = pd.read_sql(query, con)
            df_sql1 = df_sql.drop_duplicates(
                subset=None, keep='first', inplace=False, ignore_index=False)
            pub_id_merge = pd.merge(
                df_publications, df_sql1, left_on='id', right_on='doi')

            volume_issue_merge = pd.merge(
                issue_sql, volume_sql, left_on='id', right_on='id')

            result_merge = pd.merge(
                pub_id_merge, volume_issue_merge, left_on='id', right_on='id')

        return result_merge

    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        with connect("relational.db") as con:
            publication = "SELECT * FROM publications"
            df_publications = pd.read_sql(publication, con)

            query_volume = "SELECT id FROM publications WHERE volume = '%s'" % volume
            volume_sql = pd.read_sql(query_volume, con)

            query = "SELECT doi FROM issn WHERE issn = '%s'" % journalId
            df_sql = pd.read_sql(query, con)

            df_sql1 = df_sql.drop_duplicates(
                subset=None, keep='first', inplace=False, ignore_index=False)
            pub_id_merge = pd.merge(
                df_publications, df_sql1, left_on='id', right_on='doi')

            result_merge = pd.merge(
                pub_id_merge, volume_sql, left_on='id', right_on='id')

        return result_merge

    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT doi FROM issn WHERE issn = '%s'" % journalId
            df_sql = pd.read_sql(query, con)

            df_empty = pd.DataFrame()
            for i in df_sql['doi']:
                query2 = "SELECT * FROM publications WHERE id = '%s' AND type = 'journal-article'" % i
                df_sql2 = pd.read_sql(query2, con)
                df_empty = pd.concat([df_empty, df_sql2], ignore_index=True)

        return df_empty

    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT event FROM venues WHERE lower(event) LIKE '%s'" % f'%{eventPartialName}%'
            df_sql = pd.read_sql(query, con)

        return df_sql

    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query = "SELECT authororcID FROM authorslist WHERE doi = '%s'" % publicationId
            df_sql = pd.read_sql(query, con)

            df_result = pd.DataFrame()
            for ids in df_sql['authororcID']:
                query2 = "SELECT * FROM authors WHERE OrcID = '%s' " % ids
                df_sql2 = pd.read_sql(query2, con)
                df_result = pd.concat([df_result, df_sql2], ignore_index=True)

        return df_result

    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:

        with connect("relational.db") as con:
            query2 = "SELECT OrcID FROM authors WHERE lower(FamilyName) LIKE '%s' OR lower(GivenName) LIKE '%s'" % (
                f'%{authorPartialName}%', f'%{authorPartialName}%')
            df_sql2 = pd.read_sql(query2, con)

            df_empty1 = pd.DataFrame()
            for i in df_sql2['OrcID']:
                query = "SELECT doi FROM authorslist WHERE authororcID = '%s'" % i
                df_sql = pd.read_sql(query, con)
                df_empty1 = pd.concat([df_sql, df_empty1], ignore_index=True)

            df_empty = pd.DataFrame()
            for doi in df_empty1['doi']:
                query3 = "SELECT * FROM publications WHERE id = '%s'" % doi
                df_sql3 = pd.read_sql(query3, con)
                df_empty = pd.concat([df_sql3, df_empty], ignore_index=True)

            df_empty = df_empty.drop_duplicates(
                subset=['id'], keep='first', inplace=False, ignore_index=False)

        return df_empty

    def getDistinctPublishersOfPublications(self, pubIdList) -> pd.DataFrame:
        with connect("relational.db") as con:
            df_empty = pd.DataFrame()
            for doi in pubIdList:
                query = "SELECT crossref FROM publications WHERE id = '%s'" % pubIdList
                df_sql = pd.read_sql(query, con)
                df_empty = pd.concat([df_empty, df_sql], ignore_index=True)

            df_result = pd.DataFrame()
            for i in df_empty['crossref']:

                query2 = "SELECT * FROM organisations WHERE crossref = '%s'" % i
                df_sql2 = pd.read_sql(query2, con)
                df_result = pd.concat([df_result, df_sql2], ignore_index=True)

            df_result = df_result.drop_duplicates(
                subset=['crossref'], keep='first', inplace=False, ignore_index=False)
            return df_result


class TriplestoreQueryProcessor(QueryProcessor, TriplestoreProcessor):

    base_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX schema: <https://schema.org/>\n"""

    def runQuery(self, selection):
        query = TriplestoreQueryProcessor.base_query + selection
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    def getPublicationQuery(self):
        q = """ ?s schema:name ?title .
                ?s rdf:type ?type .
                ?s schema:identifier ?id .
                ?s schema:datePublished ?year .
                OPTIONAL {
                {
                SELECT * WHERE {
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier ?venueId .
                    ?venue schema:name ?venueName .
                    ?venue rdf:type ?venueType .
                    }
                    LIMIT 1
                }
                }
                OPTIONAL {
                ?s schema:issueNumber ?issue .
                }
                OPTIONAL {?s schema:volumeNumber ?volume .
                }
                OPTIONAL {?s schema:bookEdition ?chapter .}
                """
        return q

    def getURIData(self, uri):
        selection = """SELECT *
                       WHERE {
                        ?s schema:name ?title .
                        ?s schema:identifier ?id .
                        ?s schema:issueNumber ?issue .
                        ?s schema:datePublished ?year .
                        ?s rdf:type ?type .
                        ?s schema:volumeNumber ?volume .
                        ?s schema:datePublished ?year. 
                        OPTIONAL {
                        ?s schema:issueNumber ?issue .
                        }
                        OPTIONAL {?s schema:volumeNumber ?volume .
                        }
                        OPTIONAL {?s schema:bookEdition ?chapter .}
                         FILTER(?s = <%s>)
                        }""" % uri

        return self.runQuery(selection)

    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        selection = """SELECT *
                        WHERE {
                        %s
                        ?s schema:datePublished "%d". 
                        }""" % (self.getPublicationQuery(), year)

        return self.runQuery(selection)

    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        """
        Retrieve all publications linked to the author with authorId id
        """
        selection = """SELECT *
                        WHERE {
                        %s
                        ?s schema:author ?author .
                        ?author schema:identifier "%s" .  
                       }""" % (self.getPublicationQuery(), id)
        return self.runQuery(selection)

    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        """
        Retrieve the Venues linked to the publisher with publisherId id
        """
        selection = """SELECT *
                    WHERE {
                    VALUES ?type {
                        schema:Periodical schema:Book schema:Event}
                    ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s rdf:type ?type.
                    OPTIONAL {?s schema:description ?event .}
                    ?s schema:isPartOf ?publisher .
                    ?publisher schema:identifier "%s" .
                    ?publisher schema:identifier ?publisherId .
                    ?publisher schema:name ?publisherName .
                    }
                    """ % id
        return self.runQuery(selection)

    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        """
        Retrieve all publications from venue with id venueId
        """
        selection = """
                    SELECT *
                    WHERE {
                    %s
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .
                    }
                    """ % (self.getPublicationQuery(), venueId)
        return self.runQuery(selection)

    def getMostCitedPublication(self) -> pd.DataFrame:
        """
        Retrieve the publication containing the most references to itself
        """
        selection = """
                    SELECT ?title ?year ?id ?issue ?volume ?chapter ?venueId ?venueName ?venueType (COUNT(?citation) as ?citations)
                    WHERE {
                    ?s schema:name ?title .
                    ?s rdf:type ?type .
                    ?s schema:identifier ?id .
                    ?s schema:datePublished ?year .
                    OPTIONAL {
                        {
                        SELECT * WHERE {
                            ?s schema:isPartOf ?venue .
                            ?venue schema:identifier ?venueId .
                            ?venue schema:name ?venueName .
                            ?venue rdf:type ?venueType .
                        }
                        LIMIT 1
                        }
                    }
                    OPTIONAL {
                        ?s schema:issueNumber ?issue .
                    }
                    OPTIONAL {?s schema:volumeNumber ?volume .
                                }
                    OPTIONAL {?s schema:bookEdition ?chapter .}
                    ?s schema:citation ?citation
                    }
                    GROUP BY ?id ?title ?year ?issue ?volume ?chapter ?venueId ?venueName ?venueType
                    ORDER BY desc(?citations)
                    LIMIT 1
                    """
        df_sparql = self.runQuery(selection)
        return df_sparql

    def getMostCitedVenue(self) -> pd.DataFrame:
        """
        Retrieve the venue containing the largest amount of references in its publications
        """
        selection = """
                    SELECT  ?venueId ?venueName ?venueType (COUNT(?citation) as ?citations)
                    WHERE
                    {  
                    ?s schema:identifier ?id .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier ?venueId .
                    ?venue schema:name ?venueName .
                    ?venue rdf:type ?venueType .
                    ?s schema:citation ?citation .
                    }
                    GROUP BY ?venueId ?venueName ?venueType
                    """
        query = TriplestoreQueryProcessor.base_query + selection
        df_sparql = get(self.getEndpointUrl(), query, True)
        df = df_sparql[df_sparql['citations'] == df_sparql['citations'].max()]
        return df

    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articles with issueNumber equal to issue and volumneNumber equal to volumn
        from venue with id journalId
        """
        selection = """
                    SELECT *
                    WHERE {
                    %s
                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:issueNumber "%d" .
                    ?s schema:volumeNumber "%d" .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .

                    }    
                    """ % (self.getPublicationQuery(), issue, volume, journalId)
        return self.runQuery(selection)

    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articles with volume equal to volume from venue with id journalId
        """
        selection = """
                    SELECT *
                    WHERE {
                    %s
                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:volumeNumber "%d" .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .

                    }    
                    """ % (self.getPublicationQuery(), volume, journalId)
        return self.runQuery(selection)

    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articlesfrom venue with id journalId
        """
        selection = """
                    SELECT *
                    WHERE {
                    %s
                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .
                    }    
                    """ % (self.getPublicationQuery(), journalId)
        return self.runQuery(selection)

    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        selection = """
                    SELECT ?id ?title ?event
                    WHERE {
                    ?s rdf:type schema:Event .
                    ?s schema:description ?event .
                    ?s schema:name ?title .
                    ?s schema:identifier ?id .
                    filter contains(lcase(?event), "%s").
                    }
                    """ % (eventPartialName)

        return self.runQuery(selection)

    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:
        """
        Retrieve all authors of a publication with id publicationId
        """
        selection = """
                    SELECT ?id ?given ?family
                    WHERE {
                    VALUES ?type {
                        schema:ScholarlyArticle schema:Chapter }
                    ?s rdf:type ?type .
                    ?s schema:identifier "%s" .
                    ?s schema:author ?author .
                    ?author schema:identifier ?id . 
                    ?author schema:givenName ?given .
                    ?author schema:familyName ?family .
                    }
                    """ % publicationId

        return self.runQuery(selection)

    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:
        """
        Retrieve all publications linked to author of partial name authorPartialName
        """
        selection = """
                    SELECT *
                    WHERE {
                    %s
                    {
                    ?s schema:author ?author .
                    ?author schema:givenName ?given .
                    filter contains(lcase(?given), "%s").
                    }
                    UNION
                    {?s schema:author ?author .
                        ?author schema:familyName ?family .
                        filter contains(lcase(?family), "%s").
                        }
                    }
                    """ % (self.getPublicationQuery(), authorPartialName, authorPartialName)

        return self.runQuery(selection)

    def getDistinctPublishersOfPublications(self, pubIdList) -> pd.DataFrame:
        """
        Retrieve the set of distinct publishers based on a list of publication ids
        """
        df_empty = pd.DataFrame()
        for id in pubIdList:
            selection = """
                        SELECT distinct ?id ?name
                        WHERE {
                        ?s schema:identifier "%s".
                        ?s schema:isPartOf ?venue .
                        ?venue schema:isPartOf ?publisher .
                        ?publisher schema:name ?name .
                        ?publisher schema:identifier ?id .
                        
                        }
                        """ % id

            df_sparql = self.runQuery(selection)
            df = pd.concat([df_empty, df_sparql], ignore_index=True)
            df_empty = df_sparql
        return df.drop_duplicates('id')


class GenericQueryProcessor(QueryProcessor):

    def __init__(self) -> None:
        self.queryProcessors: list[QueryProcessor] = []

    def checkProcessor(self):
        return len(self.queryProcessors) != 0

    def cleanQueryProcessors(self) -> None:
        self.queryProcessors = []

    def addQueryProcessor(self, processor: QueryProcessor) -> None:
        self.queryProcessors.append(processor)

    def createPublication(self, row):
        id = row['id']
        title = row['title']
        year = row['year']
        issue = row['issue']
        volume = row['volume']
        typ = row['type']
        chapter = row['chapter']
        if URIRef(typ) == Schema.JournalArticle or typ == 'journal-article':
            model = JournalArticle(id, year, title, issue, volume)
        elif URIRef(typ) == Schema.BookChapter or typ == 'book-chapter':
            model = BookChapter(id, year, title, chapter)
        else:
            model = ProceeedingsPaper(id, year, title)

        if row['venueName']:
            venueName = row['venueName']
            venueId = row['venueId']
            venueType = row['venueType']
            if URIRef(venueType) == Schema.Journal or venueType == 'journal':
                venue = Journal(venueId, venueName)
            else:
                venue = Book(venueId, venueName)
            model.setPublicationVenue(venue)

        return model

    def getPublicationsPublishedInYear(self, year: int):
        """
        Go through all query processessors in the list and run the 
        query with same name for all and concat the resultas into one dataframe.
        Then we go through the df and add each row as a python object with same model class name
        If we retrieve a publication -> we create a python class pub = Publication(id, title, year , issue, volume, chapter etc. )
        """
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getPublicationsPublishedInYear(year)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getPublicationsByAuthorId(self, id: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getPublicationsByAuthorId(id)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getMostCitedPublication(self):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getMostCitedPublication()
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getMostCitedVenue(self):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getVenuesByPublisherId(id)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df

        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            id = row['venueId']
            title = row['venueName']
            venueType = row['venueType']
            if URIRef(venueType) == Schema.Journal:
                venue = Journal(id, title)
            elif URIRef(venueType) == Schema.Book:
                venue = Book(id, title)
            else:
                venue = Proceedings(id, title)
            p_objects.append(venue)

        return p_objects

    def getVenuesByPublisherId(self, id: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getVenuesByPublisherId(id)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df

        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            id = row['id']
            title = row['name']
            venueType = row['type']
            if URIRef(venueType) == Schema.Journal:
                venue = Journal(id, title)
            elif URIRef(venueType) == Schema.Book:
                venue = Book(id, title)
            else:

                venue = Proceedings(id, title)

            publisherId = row['publisherId']
            publisherName = row['publisherName']
            publisher = Organization(publisherId, publisherName)
            venue.setPublisher(publisher)
            p_objects.append(venue)

        return p_objects

    def getPublicationInVenue(self, venueId: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getPublicationInVenue(venueId)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getJournalArticlesInIssue(issue, volume, journalId)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getJournalArticlesInVolume(self, volume: str, journalId: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getJournalArticlesInVolume(volume, journalId)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getJournalArticlesInJournal(self, journalId: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getJournalArticlesInJournal(journalId)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getProceedingsByEvent(self, eventPartialName: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getProceedingsByEvent(eventPartialName)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            id = row['id']
            title = row['title']
            event = row['event']
            venue = Proceedings(id, title, event)
            p_objects.append(venue)
        return p_objects

    def getPublicationAuthors(self, publicationId: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getPublicationAuthors(publicationId)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            id = row['id']
            givenName = row['given']
            familyName = row["family"]
            author = Person(givenName, familyName, id)
            p_objects.append(author)
        return p_objects

    def getPublicationByAuthorName(self, authorPartialName: str):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getPublicationByAuthorName(authorPartialName)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df
        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            p_objects.append(self.createPublication(row))

        return p_objects

    def getDistinctPublishersOfPublications(self, pubIdList):
        if not self.checkProcessor():
            return "NO QUERY PROCESSORS ADDED"

        df_total = pd.DataFrame()
        p_objects = []
        for processor in self.queryProcessors:
            df = processor.getDistinctPublishersOfPublications(pubIdList)
            df = pd.concat([df_total, df], ignore_index=True)
            df_total = df

        df_total = df_total.drop_duplicates(subset=["id"])
        df_total = df_total.fillna("")
        for idx, row in df_total.iterrows():
            publisherId = row['id']
            publisherName = row['name']
            publisher = Organization(publisherId, publisherName)

            p_objects.append(publisher)

        return p_objects


# grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# grp_dp = TriplestoreDataProcessor()
# grp_dp.setEndpointUrl(grp_endpoint)
# grp_dp.uploadData("data/json_small.json")
# grp_dp.uploadData("data/publications_small.csv")
# grp_dp.uploadData("data/json_small.json")

# grd_qp = TriplestoreQueryProcessor()
# grd_qp.setEndpointUrl(grp_endpoint)

# generic = GenericQueryProcessor()
# generic.addQueryProcessor(grd_qp)
# print(generic.getPublicationsPublishedInYear(2017))
# print(generic.getPublicationInVenue("issn:0219-3116"))
# print(generic.getPublicationsByAuthorId("0000-0002-3938-2064"))

rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
# grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# grp_dp = TriplestoreDataProcessor()
# grp_dp.setEndpointUrl(grp_endpoint)
# grp_dp.uploadData("data/graph_publications.csv")
# grp_dp.uploadData("data/graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

# grp_qp = TriplestoreQueryProcessor()
# grp_qp.setEndpointUrl(grp_endpoint)

# # Finally, create a generic query processor for asking
# # about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
# generic.addQueryProcessor(grp_qp)

result_q1 = generic.getPublicationsPublishedInYear(2021)
#result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
print(result_q1)
