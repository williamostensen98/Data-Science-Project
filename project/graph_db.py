from numpy import identity
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
    event = URIRef("https://schema.org/name")
    familyName = URIRef("https://schema.org/familyName")
    givenName = URIRef("https://schema.org/givenName")

    # relations among classes
    publicationVenue = URIRef("https://schema.org/isPartOf")
    citation = URIRef("https://schema.org/citation")
    publisher = URIRef("https://schema.org/isPartOf")
    author = URIRef("https://schema.org/author")
    cites = URIRef("https://schema.org/citation")


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
        self.venues_internal_id = {}  # name: subj
        self.publisher_internal_id = {}  # id: subj
        self.author_internal_id = {}  # id: subj

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

        # Once finished, remeber to close the connection
        TriplestoreProcessor.store.close()

    def handle_csv_upload(self, path: str) -> None:
        publications = pd.read_csv(path,
                                   keep_default_na=False,
                                   dtype={
                                       "id": "string",
                                       "title": "string",
                                       "publication year": "int",
                                       "publication venue": "string",
                                       "type": "string",
                                       "issue": "string",
                                       "volume": "string",
                                       "chapter": "string",
                                       "venue_type": "string",
                                       "publisher": "string",
                                       "event": "string"
                                   })
        for idx, row in publications.iterrows():
            local_id = "publication" + str(idx)
            subject = URIRef(TriplestoreProcessor.base_url + local_id)
            self.publication_internal_id[row['id']] = subject

            if row["type"] == "journal-article":
                TriplestoreProcessor.graph.add(
                    (subject, RDF.type, Schema.JournalArticle))

                # These two statements applies only to journal articles
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
                (subject, Schema.identifier, Literal(row["id"])))

            # The original value here has been casted to string since the Date type
            # in schema.org ('https://schema.org/Date') is actually a string-like value
            TriplestoreProcessor.graph.add((subject, Schema.publicationYear, Literal(
                str(row["publication_year"]))))

    def handle_json_upload(self, path: str) -> None:
        with open(path) as f:
            d = json.load(f)

        authors = pd.json_normalize(d['authors'])
        # venues = pd.json_normalize(d['venues'])
        # publishers = pd.json_normalize(d['publishers'])
        # citations = pd.json_normalize(d['references'])
        index = 0
        # Adding authors to publications
        for doi in authors:

            for author in authors[doi][0]:
                local_id = "author-" + str(index)
                index += 1
                subject = URIRef(TriplestoreProcessor.base_url + local_id)
                self.author_internal_id[doi] = subject
                TriplestoreProcessor.graph.add(
                    (subject, RDF.type, Schema.Person))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.identifier, Literal(author['orcid'])))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.givenName, Literal(author['given'])))
                TriplestoreProcessor.graph.add(
                    (subject, Schema.familyName, Literal(author['family'])))

                if doi in self.publication_internal_id:
                    TriplestoreProcessor.graph.add(
                        (self.publication_internal_id[doi], Schema.author, subject))

    def get_file_ending(self, filename: str) -> str:
        ending = filename.split(".")
        return ending[1]


class TripletoreQueryProcessor(TriplestoreProcessor):

    base_query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX schema: <https://schema.org/>\n"""

    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        selection = """SELECT ?id ?title
WHERE {
    ?s schema:name ?title .
    ?s schema:identifier ?id .
    ?s schema:datePublished "%d". 
}""" % year

        query = TripletoreQueryProcessor.base_query + selection

        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql


grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("data/publications_small.csv")
grp_dp.uploadData("data/json_small.json")

# grd_qp = TripletoreQueryProcessor()
# grd_qp.setEndpointUrl(grp_endpoint)
# print(grd_qp.getPublicationsPublishedInYear(2014))

# SELECT ?title ?id ?year ?name
# WHERE {
#   ?s rdf:type schema:ScholarlyArticle .
#   ?s schema:name ?title .
#   ?s schema:identifier ?id .
#   ?s schema:datePublished ?year .
#   ?s schema:author ?author .
#   ?author schema:identifier "0000-0002-3938-2064" .
#   ?author schema:givenName ?name .
# }
