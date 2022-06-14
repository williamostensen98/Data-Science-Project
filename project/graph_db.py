from rdflib import Graph
from rdflib import URIRef
from rdflib import Literal
from rdflib import RDF
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
import json


class Schema():

    # classes of resources
    JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
    BookChapter = URIRef("https://schema.org/Chapter")
    Journal = URIRef("https://schema.org/Periodical")
    Book = URIRef("https://schema.org/Book")
    Publisher = URIRef("https://schema.org/Organization")
    Proceeedings = URIRef("https://schema.org/Event")
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
        self.venue_publication = {}
        self.flag = False
        self.pub_idx = 0
        self.ven_idx = 0
        self.auth_idx = 0
        self.org_idx = 0

    # TODO: Implement Triplestore upload data
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
                print("Venue already exists")
                venue = self.venues_internal_id[venue_internal_name[row["publication_venue"]]]
            elif row['id'] in self.venues_internal_id:
                venue = self.venues_internal_id[row['id']]
            else:
                print("Create venue")
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
                    (venue, RDF.type, Schema.Proceeedings))
                TriplestoreProcessor.graph.add(
                    (venue, Schema.event, row['event']))

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
        self.flag = True

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
            for id in venues[doi][0]:
                print(id)
                if id in self.venue_publication:

                    subject = self.venues_internal_id[self.venue_publication[id]]

                elif doi in self.venues_internal_id:

                    subject = self.venues_internal_id[doi]
                else:
                    # Create Venue Object
                    local_id = "venue-" + str(self.ven_idx)
                    self.ven_idx += 1
                    subject = URIRef(TriplestoreProcessor.base_url + local_id)
                    self.venues_internal_id[doi] = subject
                    self.venue_publication[id] = doi

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
                print("Citation for: ", doi, reference)
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


class TriplestoreQueryProcessor(TriplestoreProcessor):

    base_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX schema: <https://schema.org/>\n"""

    def runQuery(self, selection):
        query = TriplestoreQueryProcessor.base_query + selection
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        selection = """SELECT ?id ?title 
                        WHERE {
                            ?s schema:name ?title .
                            ?s schema:identifier ?id .
                            ?s schema:datePublished "%d". 
                        }""" % year

        return self.runQuery(selection)

    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        """
        Retrieve all publications linked to the author with authorId id
        """
        selection = """SELECT ?title ?id ?year ?name ?type
                        WHERE {
                        ?s schema:name ?title .
                        ?s schema:identifier ?id .
                        ?s schema:datePublished ?year .
                        ?s rdf:type ?type .
                        ?s schema:author ?author .
                        ?author schema:identifier "%s" . 
                        ?author schema:givenName ?name .
                        }""" % id
        return self.runQuery(selection)

    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        """
        Retrieve the Venues linked to the publisher with publisherId id
        """
        selection = """SELECT ?id ?name ?type
                    WHERE {
                    VALUES ?type {
                        schema:Periodical schema:Book }
                        ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s rdf:type ?type.
                    ?s schema:isPartOf ?publisher .
                    ?publisher schema:identifier "%s" .
                    }
                    """ % id
        return self.runQuery(selection)

    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        """
        Retrieve all publications from venue with id venueId
        """
        selection = """
                    SELECT ?id ?name
                    WHERE {
                    VALUES ?type {
                        schema:ScholarlyArticle schema:Chapter }
                        ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s rdf:type ?type.
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .
                    }
                    """ % venueId
        return self.runQuery(selection)

    def getMostCitedPublication(self) -> pd.DataFrame:
        """
        Retrieve the publication containing the most references to itself
        """
        selection = """
                    SELECT ?id (COUNT(?id) as ?citations)
                    WHERE
                    {
                    ?s schema:identifier ?id .
                    ?s schema:citation ?citation
                    }
                    GROUP BY ?id   
                    """
        df_sparql = self.runQuery(selection)
        df = df_sparql[df_sparql['citations'] == df_sparql['citations'].max()]
        return df

    def getMostCitedVenue(self) -> pd.DataFrame:
        """
        Retrieve the venue containing the largest amount of references in its publications
        """
        selection = """
                    SELECT  ?venueId (COUNT(?citation) as ?citations)
                    WHERE
                    {  
                    ?s schema:identifier ?id .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier ?venueId .
                    ?s schema:citation ?citation .
                    }
                    GROUP BY ?venueId
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
                    SELECT ?id ?name
                    WHERE {

                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s schema:issueNumber "%d" .
                    ?s schema:volumeNumber "%d" .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .

                    }    
                    """ % (issue, volume, journalId)
        return self.runQuery(selection)

    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articles with volume equal to volume from venue with id journalId
        """
        selection = """
                    SELECT ?id ?name
                    WHERE {

                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s schema:volumeNumber "%d" .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .

                    }    
                    """ % (volume, journalId)
        return self.runQuery(selection)

    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articlesfrom venue with id journalId
        """
        selection = """
                    SELECT ?id ?name
                    WHERE {

                    ?s rdf:type schema:ScholarlyArticle .
                    ?s schema:name ?name .
                    ?s schema:identifier ?id .
                    ?s schema:isPartOf ?venue .
                    ?venue schema:identifier "%s" .

                    }    
                    """ % journalId
        return self.runQuery(selection)

    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        selection = """
                    SELECT ?id ?event
                    WHERE {
                    ?s rdf:type schema:Periodical .
                    ?s schema:description ?event .
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
                    SELECT ?id ?title
                    WHERE {
                    ?s schema:name ?title .
                    ?s schema:identifier ?id .
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
                    """ % (authorPartialName, authorPartialName)

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


grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# grp_dp = TriplestoreDataProcessor()
# grp_dp.setEndpointUrl(grp_endpoint)
# grp_dp.uploadData("data/json_small.json")
# grp_dp.uploadData("data/publications_small.csv")
# grp_dp.uploadData("data/json_small.json")

grd_qp = TriplestoreQueryProcessor()
grd_qp.setEndpointUrl(grp_endpoint)
print(grd_qp.getPublicationsPublishedInYear(2021))
print(grd_qp.getPublicationsByAuthorId("0000-0002-3938-2064"))
print(grd_qp.getMostCitedVenue())
print(grd_qp.getVenuesByPublisherId("crossref:78"))
print(grd_qp.getDistinctPublishersOfPublications(
    ["doi:10.1016/j.websem.2021.100655", "doi:10.1016/j.websem.2014.03.003", "doi:10.1007/s10115-017-1100-y"]))
