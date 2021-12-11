
from neo4j import GraphDatabase, basic_auth
class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        
        self.uri = uri
        self.user = user
        self.pwd = pwd
        self.driver = None
        
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.pwd))
        except Exception as e:
            print(e)
        
    def close(self):
        
        if self.driver is not None:
            self.driver.close()
        
    def query(self, query, parameters=None, db=None):
        
        assert self.river is not None, "Driver not initialized!"
        session = None
        response = None
        
        try: 
            session = self.driver.session(database=db) if db is not None else self.driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
