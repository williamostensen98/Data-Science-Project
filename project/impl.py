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

    def __init__(self) -> None:
        super().__init__()

    # TODO: Implement upload Data
    def uploadData(self, path: str) -> None:
        
        def add_internalID(df, id_name):
        internal_id = []
            for idx, row in df.iterrows():
                internal_id.append(id_name + "-" + str(idx))

            df.insert(0, id_name + "_internalID", Series(internal_id, dtype="string"))
                
            return df

        ## DATAFRAME AUTHORS & AUTHOR_LIST
        with open("/Users/TomKobes/Documents/Unibo/DataScience/Data/data.json", "r", encoding="utf-8") as f:
            json_doc = load(f)
            doi_authors = json_doc["authors"]

            df_author = pd.DataFrame({'family': [], 'given': [], 'orcid': []})
            df_doi_orcid = pd.DataFrame({'doi': [], 'orcid': []})
            
            for doi in doi_authors: 
                    
                for author in doi_authors[doi]:
                    
                    new_row_author = pd.DataFrame(author, index=[0])
                    df_author = pd.concat([new_row_author, df_author.loc[:]]).reset_index(drop=True)
                    
                    dict = {'doi': doi, 'orcid': author['orcid']}
                    new_row_orcid = pd.DataFrame(dict, index=[0])
                    df_doi_orcid = pd.concat([new_row_orcid, df_doi_orcid.loc[:]]).reset_index(drop=True)
                
            df_author = df_author.drop_duplicates(subset = ['orcid'], keep = 'last').reset_index(drop = True)  
            
            

        ## DATAFRAME PUBLISHERS
        with open("/Users/TomKobes/Documents/Unibo/DataScience/Data/data.json", "r", encoding="utf-8") as f:
            json_doc = load(f)
            publishers = json_doc["publishers"]

            l_name = [] 
            l_crossref = [] 
            
            for ref in publishers: # make 2-col DataFrame; doi, dict authours
                l_name.append(publishers[ref]["name"])
                l_crossref.append(publishers[ref]['id'])
            
            df = DataFrame({'name' : pd.Series(l_name), 'crossref': pd.Series(l_crossref)})
            df_organisation = add_internalID(df, 'organisation')

        ## DATAFRAME VENUES
        df_venues = read_csv("/Users/TomKobes/Documents/Unibo/DataScience/Data/rp.csv", 
                                keep_default_na=False,
                                dtype={
                                    "doi": "string",
                                    "title": "string",
                                    "type": "string",
                                    "publication year": "int",
                                    "issue": "string",
                                    "volume": "string",
                                    "chapter": "string",
                                    "publication venue": "string",
                                    "venue type": "string",
                                    "publisher": "string",
                                    "event": "string"
                                })

        venues = df_venues[['publication_venue', 'venue_type', 'event']]

        venues_distinct = venues.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
        venues = venues_distinct.reset_index(drop=True)
        df_venues = add_internalID(venues, 'venue')

        ## DATAFRAME ISSN
        with open("/Users/TomKobes/Documents/Unibo/DataScience/Data/data.json", "r", encoding="utf-8") as f:
            json_doc = load(f)
            issn = json_doc["venues_id"]
            
            l_doi = []
            l_issn = []
            
            for key in issn:
                
                for i in issn[key]:
                    l_issn.append(i)
                    l_doi.append(key)
                
            df_issn = pd.DataFrame({"issn": pd.Series(l_issn), "doi": pd.Series(l_doi)})

        with open("/Users/TomKobes/Documents/Unibo/DataScience/Data/data.json", "r", encoding="utf-8") as f:
            json_doc = load(f)
            references = json_doc["references"]
            
            l_doi = []
            l_references = []
            
            for key in references:
                
                for i in references[key]:
                    l_doi.append(i)
                    l_references.append(key)
            
            df_references = pd.DataFrame({"doi": pd.Series(l_doi), "referes_to": pd.Series(l_references)})

        df_publications = read_csv("/Users/TomKobes/Documents/Unibo/DataScience/Data/rp.csv", 
                                keep_default_na=False,
                                dtype={
                                    "doi": "string",
                                    "title": "string",
                                    "type": "string",
                                    "publication year": "int",
                                    "issue": "string",
                                    "volume": "string",
                                    "chapter": "string",
                                    "publication venue": "string",
                                    "venue type": "string",
                                    "publisher": "string",
                                    "event": "string"
                                })

        # Create a new column with internal identifiers for each publication
        publication_internal_id = []
        for idx, row in df_publications.iterrows():
            publication_internal_id.append("publication-" + str(idx))
        df_publications.insert(0, "internalId", Series(publication_internal_id, dtype="string"))

        #TODO: Don't lose the publications without crossref
        pub_org_merge = pd.merge(df_publications, df_organisation, left_on='publisher', right_on='crossref')

        pub_final2 = pub_org_merge.drop(labels=['publisher', 'organisation_internalID'], axis=1)
        pub_final2 = pub_final2.rename(columns={"internalId": "internal_publicationID", "name": "publisher"})


        #### BUILD AND FILL DATABASE!!

        with connect("publications.db") as con:
            
            con.commit()


        with connect("publications.db") as con:
            
            df_organisation.to_sql("organisations", con, if_exists="replace", index=False)
            df_author.to_sql("authors", con, if_exists="replace", index=False)
            df_doi_orcid.to_sql("authorslist", con, if_exists='replace', index=False)
            pub_final2.to_sql("publications", con, if_exists='replace', index=False)
            df_venues.to_sql("venues", con, if_exists='replace', index=False)
            df_references.to_sql("references_to", con, if_exists='replace', index=False)
            df_issn.to_sql("issn", con, if_exists='replace', index=False)


class TriplestoreProcessor():

    def __init__(self) -> None:
        self.endpointUrl = ""

    def getEndpointUrl(self) -> str:
        return self.endpointUrl

    def setEndpointUrl(self, url: str) -> None:
        self.endpointUrl = url


class TriplestoreDataProcessor(TriplestoreProcessor):

    def __init__(self) -> None:
        super().__init__()

    # TODO: Implement Triplestore upload data
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
       
        with connect("publications.db") as con: 
            query = "SELECT * FROM publications WHERE publication_year = %d" % year 
            df_sql = read_sql(query, con)
            
        return df_sql


    # TODO: Implement relational getPublicationsByAuthorId
    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        
        with connect("publications.db") as con: 
            query = "SELECT doi FROM authorslist WHERE authororcID = '%s'" %id
            df_sql = read_sql(query, con)   

        return df_sql

    # TODO: Implement relational getMostCitedPublication
    def getMostCitedPublication(self) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT referes_to, COUNT(*) FROM references_to GROUP BY referes_to"
            df_sql = read_sql(query, con)
            
            max = df_sql.loc[df_sql['COUNT(*)'].idxmax()]
            max = max['referes_to']
            
            df_max = pd.DataFrame({"doi": pd.Series(max)})
            
            result = pd.merge(df_publications, df_max, left_on='id', right_on='doi')
        return result
        

    # TODO: Implement relational getMostCitedVenue
    def getMostCitedVenue(self) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT publication_venue, id FROM publications"
            df_sql = read_sql(query, con)
            
            query2 = "SELECT referes_to, COUNT(*) FROM references_to GROUP BY referes_to"
            df_sql2 = read_sql(query2, con)
            
            df_result = pd.merge(df_sql, df_sql2, left_on='id', right_on='referes_to')
            df_result = df_result.groupby(by=["publication_venue"]).sum().reset_index()
            
            max = df_result.loc[df_result['COUNT(*)'].idxmax()]
            result_venue = max['publication_venue']
            
            query3 = "SELECT * FROM venues WHERE publication_venue = '%s'" %result_venue
            df_sql3 = read_sql(query3, con)
            
        return df_sql3

    # TODO: Implement relational getVenuesByPublisherId
    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        
        with connect("publications.db") as con: 
            query = "SELECT publication_venue FROM publications WHERE crossref = '%s'" %id 
            df_sql = read_sql(query, con)
            df_sql1 = df_sql.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
        
        return df_sql1

    # TODO: Implement relational getPublicationInVenue
    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT * FROM publications WHERE issn = '%s'" %venueId          
            df_sql = read_sql(query, con)
            
            df_sql1 = df_sql.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
        
        return df_sql

    # TODO: Implement relational getJournalArticlesInIssue
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getJournalArticlesInVolume
    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        pass

    # TODO: Implement relational getJournalArticlesInJournal
    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT doi FROM issn WHERE issn = '%s'" %journalId
            df_sql = read_sql(query, con)
            
            df_empty = pd.DataFrame()
            for i in df_sql['doi']:
                query2 = "SELECT * FROM publications WHERE id = '%s' AND type = 'journal-article'" %i
                df_sql2 = read_sql(query2, con)
                df_empty = concat([df_empty, df_sql2], ignore_index=True)

        return df_empty

    # TODO: Implement relational getProceedingsByEvent
    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT event FROM venues WHERE lower(event) LIKE '%s'" %f'%{eventPartialName}%'
            df_sql = read_sql(query, con)
        
        return df_sql

    # TODO: Implement relational getPublicationAuthors
    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:
        
        with connect("publications.db") as con:
            query = "SELECT authororcID FROM authorslist WHERE doi = '%s'" %publicationId 
            df_sql = read_sql(query, con)
            
            df_result = pd.DataFrame()
            for ids in df_sql['authororcID']:
                query2 = "SELECT * FROM authors WHERE OrcID = '%s' " %ids
                df_sql2 = read_sql(query2, con)
                df_result = concat([df_result, df_sql2], ignore_index=True)
        
        return df_result

    # TODO: Implement relational getPublicationByAuthorName
    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:
        
        with connect("publications.db") as con: ## getPublicationByAuthorName
            query2 = "SELECT OrcID FROM authors WHERE lower(FamilyName) LIKE '%s' OR lower(GivenName) LIKE '%s'" % (f'%{authorPartialName}%', f'%{authorPartialName}%')
            df_sql2 = read_sql(query2, con) 
            
            df_empty1 = pd.DataFrame()
            for i in df_sql2['OrcID']:
                query = "SELECT doi FROM authorslist WHERE authororcID = '%s'" %i
                df_sql = read_sql(query, con)
                df_empty1 = concat([df_sql, df_empty1], ignore_index=True)
            
            df_empty = pd.DataFrame()
            for doi in df_empty1['doi']:
                query3 = "SELECT * FROM publications WHERE id = '%s'" %doi
                df_sql3 = read_sql(query3, con)
                df_empty = concat([df_sql3, df_empty], ignore_index=True)
        
            df_empty = df_empty.drop_duplicates(subset=['id'], keep='first', inplace=False, ignore_index=False)   
         
        return df_empty

    # TODO: Implement relational getDistinctPublishersOfPublications
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)) -> pd.DataFrame:
        with connect("publications.db") as con:
        
        df_empty = pd.DataFrame()
        for doi in pubIdList:
            query = "SELECT crossref FROM publications WHERE id = '%s'" %pubIdList
            df_sql = read_sql(query, con)
            df_empty = concat([df_empty, df_sql], ignore_index=True)
            
        df_result = pd.DataFrame()
        for i in df_empty['crossref']:
         
            query2 = "SELECT * FROM organisations WHERE crossref = '%s'" %i
            df_sql2 = read_sql(query2, con)
            df_result = concat([df_result, df_sql2], ignore_index=True)
            
        df_result = df_result.drop_duplicates(subset=['crossref'], keep='first', inplace=False, ignore_index=False)
        return df_result


class TripletoreQueryProcessor(QueryProcessor, TriplestoreProcessor):

    # TODO: Implement triplestore getPublicationsPublishedInYear
    def getPublicationsPublishedInYear(self, year: int) -> pd.DataFrame:
        """
        Retrieve all publications published in the year 'year'
        """
        pass

    # TODO: Implement triplestore getPublicationsByAuthorId
    def getPublicationsByAuthorId(self, id: str) -> pd.DataFrame:
        """
        Retrieve all publications linked to the author with authorId id
        """
        pass

    # TODO: Implement triplestore getMostCitedPublication
    def getMostCitedPublication(self) -> pd.DataFrame:
        """
        Retrieve the publication containing the most references to itself
        """
        pass

    # TODO: Implement triplestore getMostCitedVenue
    def getMostCitedVenue(self) -> pd.DataFrame:
        """
        Retrieve the venue containing the largest amount of references in its publications
        """
        pass

    # TODO: Implement triplestore getVenuesByPublisherId
    def getVenuesByPublisherId(self, id: str) -> pd.DataFrame:
        """
        Retrieve the Venues linked to the publisher with publisherId id
        """
        pass

    # TODO: Implement triplestore getPublicationInVenue
    def getPublicationInVenue(self, venueId: str) -> pd.DataFrame:
        """
        Retrieve all publications from venue with id venueId
        """
        pass

    # TODO: Implement triplestore getJournalArticlesInIssue
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articles with issueNumber equal to issue and volumneNumber equal to volumn
        from venue with id journalId
        """
        pass

    # TODO: Implement triplestore getJournalArticlesInVolume
    def getJournalArticlesInVolume(self, volume: str, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articles with volume equal to volume from venue with id journalId
        """
        pass

    # TODO: Implement triplestore getJournalArticlesInJournal
    def getJournalArticlesInJournal(self, journalId: str) -> pd.DataFrame:
        """
        Retrieve the journal articlesfrom venue with id journalId
        """
        pass

    # TODO: Implement triplestore getProceedingsByEvent
    def getProceedingsByEvent(self, eventPartialName: str) -> pd.DataFrame:
        pass

    # TODO: Implement triplestore getPublicationAuthors
    def getPublicationAuthors(self, publicationId: str) -> pd.DataFrame:
        """
        Retrieve all authors of a publication with id publicationId
        """
        pass

    # TODO: Implement triplestore getPublicationByAuthorName
    def getPublicationByAuthorName(self, authorPartialName: str) -> pd.DataFrame:
        """
        Retrieve all publications linked to author of partial name authorPartialName
        """
        pass

    # TODO: Implement triplestore getDistinctPublishersOfPublications
    def getDistinctPublishersOfPublications(self, pubIdList: list(str)) -> pd.DataFrame:
        """
        Retrieve the set of distinct publishers based on a list of publication ids
        """
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
