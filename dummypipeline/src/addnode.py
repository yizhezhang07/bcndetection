from neo4j import GraphDatabase

class AddNode:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
    
    def init_db(self):
        print("Initiating Database.....")
        with self.driver.session() as session:
            res = session.write_transaction(self._init_db,)
            print(res)
        
    @staticmethod
    def _init_db(tx, ):
        result = tx.run("CREATE CONSTRAINT IPNameConstraint ON (n:IP) ASSERT n.name IS UNIQUE")
        result = tx.run("CREATE CONSTRAINT domainNameConstraint ON (n:Domain) ASSERT n.name IS UNIQUE")
        result = tx.run("CREATE CONSTRAINT FQDNNameConstraint ON (n:FQDN) ASSERT n.name IS UNIQUE")        
    
        return result.single()
        
    ## add node  
    def add_nodes(self, file):
        ## Add nodes and relationships
        with self.driver.session() as session:
            res = session.write_transaction(self._addnode, file)
            print(res)

    @staticmethod
    def _addnode(tx, file):
        print("------Adding Nodes------")
        result = tx.run("LOAD CSV WITH HEADERS FROM $file AS csvLine \
        MERGE (f:FQDN {name: csvLine.host}) \
        ON CREATE SET f.firstDetect = date(csvLine.logday), f.isIP=toBoolean(csvLine.isIP),\
        f.lastDetect=date(csvLine.logday), f.new=True, f.cisco=toFloat(csvLine.cisco_score) \
        ON MATCH SET f.lastDetect=date(csvLine.logday), f.cisco=toFloat(csvLine.cisco_score) \
        MERGE (d:Domain {name: csvLine.domain}) \
        ON CREATE SET d.new = True, d.firstDetect = date(csvLine.logday),d.lastDetect=date(csvLine.logday) \
        ON MATCH SET d.lastDetect = date(csvLine.logday) \
        MERGE (n:IP {name: csvLine.id_resp_h})\
        ON CREATE SET n.new = True, n.firstDetect = date(csvLine.logday),n.lastDetect=date(csvLine.logday) \
        ON MATCH SET n.lastDetect = date(csvLine.logday) \
        MERGE (f)-[:REG]->(d) \
        MERGE (d)-[:HOST]->(n) \
        RETURN count(f)", file=file)
        print("------Finish Adding Nodes------")
        return result.single()
    
    ## cleanup everything
    def del_all(self):
        with self.driver.session() as session:
            res = session.write_transaction(self._del_all)
            print(res)
            
    @staticmethod
    def _del_all(tx):
        print("-------WARNING: DELETING EVERYTHING--------")
        result = tx.run("MATCH (n) DETACH DELETE n RETURN count(n)")
        print("-------Finish DELETING EVERYTHING--------")
        return result.single()
    
    ## delete node last detected on date = datestr        
    def clean_outdated_benign_nodes(self, datestr):
        ## Delete benign nodes whose lastdetect == date
        with self.driver.session() as session:
            res = session.write_transaction(self._clean_node, datestr)
            print(res)

    @staticmethod
    def _clean_node(tx, datestr):
        print("-------Cleaning DataBase--------")
        result = tx.run("MATCH (n:FQDN) WHERE n.lastDetect=date($datestr) and n.malicious = 0 DETACH DELETE n RETURN count(n)", datestr=datestr)
        
        result = tx.run("MATCH (n:FQDN) WHERE n.lastDetect=date($datestr) and not exists(n.malicious) DETACH DELETE n RETURN count(n)", datestr=datestr) 
        
        result = tx.run("MATCH (n:Domain) WHERE n.lastDetect=date($datestr) and NOT (:FQDN)-[:REG]->(n) DETACH DELETE n RETURN count(n)", datestr=datestr) 
        
        result = tx.run("MATCH (n:IP) WHERE n.lastDetect=date($datestr) and NOT (:Domain)-[:HOST]->(n) DETACH DELETE n RETURN count(n)", datestr=datestr) 
        
        print("-------Finish Cleaning DataBase--------")

        return result.single()

    
    ## update graphdb based on feedbacks
    def update_labels(self, file):
        with self.driver.session() as session:
            res = session.write_transaction(self._update_labels, file)
            print(res)

    @staticmethod
    def _update_labels(tx, file):
        print("----------Updating GraphDB Lables---------")
        result = tx.run("LOAD CSV WITH HEADERS FROM $file AS csvLine \
        MERGE (f:FQDN {name: csvLine.host}) \
        ON MATCH SET f.malFLAG=toFloat(csvLine.label), f.malEng=toInteger(csvLine.total_maleng) \
        RETURN count(f)", file=file)
        print("----------Finish Updating GraphDB Lables---------")        
        return result.single()
    
    
    ## update domain malicious features  
    def update_domMal_feats(self,):
        with self.driver.session() as session:
            res = session.write_transaction(self._updateDomMal,)
            print(res)

    @staticmethod
    def _updateDomMal(tx,):
        print("----------Updating GraphDB Domain Features---------")
        ## for each domain count its malicious neighbored FQDNs
        result = tx.run("MATCH (f:FQDN)-[:REG]->(d:Domain) where f.malFLAG>0 \
        WITH d, count(f) as cntMalFQDN SET d.cntMalFQDN = cntMalFQDN RETURN count(d)")
        
        ## for each domain compute the statistics of popularity/malicious score of its neighbored FQDNs
        result = tx.run("MATCH (f:FQDN)-[:REG]->(d:Domain) \
        WITH d, count(f) as cntFQDN, sum(f.malEng) as sumMalEng, max(f.malEng) as maxMalEng, \
        avg(f.malEng) as avgMalEng, min(f.malEng) as minMalEng, max(f.cisco) as maxCisco, \
        min(f.cisco) as minCisco, avg(f.cisco) as avgCisco, sum(f.malFLAG) as sumMal, \
        max(f.malFLAG) as maxMal, avg(f.malFLAG) as avgMal, min(f.malFLAG) as minMal \
        SET d.cntFQDN = cntFQDN, d.sumMalEng = sumMalEng, d.maxMalEng = maxMalEng, d.avgMalEng = avgMalEng,\
        d.minMalEng = minMalEng, d.maxCisco = maxCisco, d.minCisco = minCisco, d.avgCisco=avgCisco, \
        d.sumMal = sumMal, d.maxMal = maxMal, d.minMal = minMal, d.avgMal = avgMal \
        RETURN count(d)")
        print("----------Finish Updating GraphDB Domain Features---------")
        return result.single()

    ## update ip malicious features  
    def update_ipMal_feats(self,):
        with self.driver.session() as session:
            res = session.write_transaction(self._updateIPMal, )
            print(res)
    
    
    @staticmethod
    def _updateIPMal(tx,):
        print("----------Updating GraphDB IP Features---------")
        ## for each IP count its malicious neighbored domains, i.e. domain has at least one malicious FQDN
        result = tx.run("MATCH (d:Domain)-[:HOST]->(i:IP) where d.cntMalFQDN>0 \
        WITH i, count(d) as cntMalDom SET i.cntMalDom = cntMalDom \
        RETURN count(i)")
        
        ## for each IP compute the statistics of popularity/malicious score of its neighbored domains
        result = tx.run("MATCH (d:Domain)-[:HOST]->(i:IP) \
        WITH i, sum(d.avgMalEng) as sumIPDomMalEng, max(d.avgMalEng) as maxIPDomMalEng, \
        avg(d.avgMalEng) as avgIPDomMalEng, min(d.avgMalEng) as minIPDomMalEng, \
        max(d.avgCisco) as maxIPDomCisco, min(d.avgCisco) as minIPDomCisco, avg(d.avgCisco) as avgIPDomCisco, \
        sum(d.avgMal) as sumIPMalDom, max(d.avgMal) as maxIPMalDom, max(d.maxMal) as mMaxIPMalDom,\
        avg(d.avgMal) as avgIPMalDom, min(d.avgMal) as minIPMalDom \
        SET i.sumIPDomMalEng = sumIPDomMalEng, i.maxIPDomMalEng = maxIPDomMalEng, i.avgIPDomMalEng = avgIPDomMalEng, \
        i.minIPDomMalEng = minIPDomMalEng, i.maxIPDomCisco = maxIPDomCisco, i.minIPDomCisco = minIPDomCisco, i.avgIPDomCisco = avgIPDomCisco,\
        i.sumIPMalDom = sumIPMalDom, i.maxIPMalDom = maxIPMalDom, i.mMaxIPMalDom = mMaxIPMalDom, \
        i.minIPMalDom = minIPMalDom, i.avgIPMalDom = avgIPMalDom RETURN count(i)")
        
        print("----------Finish Updating GraphDB IP Features---------")
        return result.single()
    
    ## save domain feats to csv
    def domscore_to_csv(self, savefpath):
        with self.driver.session() as session:
            res = session.write_transaction(self._domscore_to_csv, savefpath)
            print(res)

    @staticmethod
    def _domscore_to_csv(tx, savefpath):
        print("----------Writing GraphDB Domain Features---------")
        result = tx.run("WITH 'Match (f:FQDN)-[:REG]->(n:Domain) \
                        return n.name as domain, n.cntMalFQDN as cntMalFQDNs, size((n)-[:HOST]->()) as cntIP,\
                        n.cntFQDN as cntFQDN, n.sumMalEng as sumMalEng, n.maxMalEng as maxMalEng,\
                        n.avgMalEng as avgMalEng, n.minMalEng as minMalEng, n.maxCisco as maxCisco, \
                        n.minCisco as minCisco, n.avgCisco as avgCisco, n.sumMal as sumMal, \
                        n.maxMal as maxMal, n.minMal as minMal, n.avgMal as avgMal' as query \
                        call apoc.export.csv.query(query, $savefpath, {}) \
                        yield file, source, format, nodes, relationships, properties, time, rows, batchSize \
                        return file, source, format, nodes, relationships, properties, time, rows, batchSize;", savefpath=savefpath)
        print("----------Finish Writing GraphDB Domain Features---------")        
        return result.single()
    
    
    ## save ip feats to csv
    def ipscore_to_csv(self, savefpath):
        with self.driver.session() as session:
            res = session.write_transaction(self._ipscore_to_csv, savefpath)
            print(res)

    @staticmethod
    def _ipscore_to_csv(tx, savefpath):
        print("----------Writing GraphDB IP Features---------")
        result = tx.run("WITH 'Match (d:Domain)-[:HOST]->(i:IP) \
        return i.name as ip, i.cntMalDom as cntMalDom, count(d) as cntDom, \
        i.sumIPDomMalEng as sumIPDomMalEng, i.maxIPDomMalEng as maxIPDomMalEng, i.minIPDomMalEng as minIPDomMalEng,\
        i.avgIPDomMalEng as avgIPDomMalEng, i.maxIPDomCisco as maxIPDomCisco, i.minIPDomCisco as minIPDomCisco, i.avgIPDomCisco as avgIPDomCisco, \
        i.sumIPMalDom as sumIPMalDom, i.maxIPMalDom as maxIPMalDom, i.avgIPMalDom as avgIPMalDom, \
        i.minIPMalDom as minIPMalDom, i.mMaxIPMalDom as mMaxIPMalDom' as query \
        call apoc.export.csv.query(query, $savefpath, {}) \
        yield file, source, format, nodes, relationships, properties, time, rows, batchSize\
        return file, source, format, nodes, relationships, properties, time, rows, batchSize;", savefpath=savefpath)
        print("----------Finish Writing GraphDB IP Features---------")        
        return result.single()
    
    
    # find isoloated fqdn clusters
    def isolated_fqdn(self, savefpath):
        with self.driver.session() as session:
            res = session.write_transaction(self._isolated_fqdn, savefpath)
            print(res)

    @staticmethod
    def _isolated_fqdn(tx, savefpath):
        print("----------Writing GraphDB Isolated FQDNs---------")        
        result = tx.run("with 'Match (n:FQDN)-[:REG]->(d:Domain)-[:HOST]->(i:IP) \
        where n.new=true and d.new=true and i.new=true \
        return n.name as fqdn, d.name as domain, i.name as ip' as query\
        call apoc.export.csv.query(query, $savefpath, {}) \
        yield file, source, format, nodes, relationships, properties, time, rows, batchSize\
        return file, source, format, nodes, relationships, properties, time, rows, batchSize;", savefpath=savefpath)        
        print("----------Finish Writing GraphDB Isolated FQDNs---------")                        
        return result.single()
    
    # find newly added fqdns
    def new_fqdn(self, savefpath):
        with self.driver.session() as session:
            res = session.write_transaction(self._new_fqdn, savefpath)
            print(res)

    @staticmethod
    def _new_fqdn(tx, savefpath):
        print("----------Writing GraphDB New FQDNs---------")        
        result = tx.run("with 'Match (n:FQDN) where n.new=true return n.name as fqdn' as query                         call apoc.export.csv.query(query, $savefpath, {})                         yield file, source, format, nodes, relationships, properties, time, rows, batchSize                         return file, source, format, nodes, relationships, properties, time, rows, batchSize;", savefpath=savefpath)
        
        print("----------Finish Writing GraphDB New FQDNs---------")           
        return result.single()
    
    # set flag new to False before next day
    def update_flags(self):
        with self.driver.session() as session:
            res = session.write_transaction(self._update_flags)
            print(res)

    @staticmethod
    def _update_flags(tx):
        print("----------Updating GraphDB New FQDNs Flags---------")        
        result = tx.run("MATCH (n) SET n.new=False RETURN count(n)")
        print("----------Finish Updating GraphDB New FQDNs Flags---------")        
        return result.single()

    """
    ## delete node last detected on date 
    def cleancache(self, datestr):
        with self.driver.session() as session:
            res = session.write_transaction(self._cleancache, datestr)
            print(res)

    @staticmethod
    def _cleancache(tx, datestr):
        print("------Cleaning GraphDB Cache------")
        result = tx.run("MATCH (n) WHERE n.uvaFirstDetect=date($datestr) DETACH DELETE n RETURN count(n)", datestr=datestr)        
        
        result = tx.run("MATCH (n) WHERE n.vtFirstDetect=date($datestr) DETACH DELETE n RETURN count(n)", datestr=datestr)
        
        result = tx.run("MATCH (n) WHERE n.aggFirstDetect=date($datestr) DETACH DELETE n RETURN count(n)", datestr=datestr)
        print("------Finish Cleaning GraphDB Cache------")
        return result.single()
    """
