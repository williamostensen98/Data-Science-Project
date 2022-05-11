class RelationalDataProcessor():

    def __init__(self, dbPath) -> None:
        self.dbPath = dbPath

    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, newPath):
        self.dbPath = newPath
