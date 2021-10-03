import datetime

import pandas as pd
import requests
from py2neo import Graph, Relationship, NodeMatcher
from sqlalchemy import create_engine


# adding labels and relationships - this code is designed to chunk it into little memory-safe units.
# the only safe way to do it is to pull the query in MySQL and then write the output to file
# where Python can go get it in little bits.

# here's the labeler. It's really, really slow. I thought I had it working with py2neo add_label
# but the committed records just weren't sticking. I'm baffled and have resorted to brute force for now.
# takes about 3h to run.

def start_timer():
    start = datetime.datetime.now()
    print(start)
    return start


def show_finish(start_time):
    finish = datetime.datetime.now()
    print(finish)
    print(finish - start_time)


def label_nodes(graph):
    start = start_timer()
    ctr = 0

    step = 1000

    # add labels to concepts

    for chunk in pd.read_csv('/Volumes/kgweber/umls_neo4j_load/semtypes.pipe', sep='|', chunksize=step):
        grouped = chunk.groupby('SEMTYPE')['CUI'].apply(list)
        for index, row in grouped.iteritems():
            graph.run("match (n) where n.cui in {} set n :{} return n".format(row, index)).to_table()
            print(index)
        ctr += step
        print(ctr)

    show_finish(start)


# this block of code builds the relationships between nodes from the UMLS concept tree.
def build_umls_rels(graph, node_matcher):

    start = start_timer()

    ctr = 0

    step = 1000

    for chunk in pd.read_csv('/Volumes/kgweber/umls_neo4j_load/rels.pipe', sep='|', chunksize=step):

        tx = graph.begin()

        for index, row in chunk.iterrows():
            n1 = node_matcher.match("Concept", cui=row['CUI1']).first()
            n2 = node_matcher.match("Concept", cui=row['CUI2']).first()
            if n1 is None or n2 is None or row['RELA'] is None:
                continue
            else:
                r = Relationship(n2, row['RELA'].upper(), n1)
                r['rel'] = row['REL']
                print(n1, n2, ctr, index, r)
                tx.merge(r)
        tx.commit()
        print('committed')
        ctr += step
        print(ctr)

    show_finish(start)


# Code for getting the RXClass mappings to get us from drugs to
# diseases and syndromes they treat
def map_rxclasses(graph, node_matcher, umls_db_engine):

    start = start_timer()

    rxclass_base = 'https://rxnav.nlm.nih.gov/REST/rxclass/'

    # go get the diseases rxclass cares about

    # drugs I care about
    used_drugs = graph.run(
        "MATCH (c:Concept{vocabulary:'RXNORM'})-[:RELATED_TO]-(:PItem) return distinct c.str, c.sourceCUI").to_data_frame()

    classlist = []

    for index, drug in used_drugs.iterrows():
        print(drug['c.str'])
        drugrels = requests.get(rxclass_base +
                                'class/byRxcui.json?rxcui={}&relaSource=MEDRT&relas=may_treat'
                                .format(drug['c.sourceCUI']))
        if drugrels.status_code == 200:

            classes = drugrels.json()
            if 'rxclassDrugInfoList' in classes.keys():
                for details in classes['rxclassDrugInfoList']['rxclassDrugInfo']:
                    classlist.append({
                        'drugRxNorm': drug['c.sourceCUI'],
                        'classId': details['rxclassMinConceptItem']['classId'],
                        'className': details['rxclassMinConceptItem']['className'],
                        'classType': details['rxclassMinConceptItem']['classType'],
                        'rela': details['rela'],
                        'relaSource': details['relaSource']})

    class_df = pd.DataFrame(classlist).drop_duplicates()
    class_df['class_cui'] = ''

    for index, row in class_df.iterrows():
        cuis = umls_db_engine.execute("""
            SELECT DISTINCT CUI from umls.MRCONSO
            where CODE = '{}' and STT = 'PF' and LAT = 'ENG' and TS = 'P'
        """.format(row['classId'])).fetchall()
        if len(cuis) > 0:
            for cui in cuis[0]:
                tx = graph.begin()

                n1 = node_matcher.match("Concept", cui=cui).first()
                n2 = node_matcher.match("Concept", sourceCUI=row['drugRxNorm']).first()
                if n1 is None or n2 is None or row['rela'] is None:
                    print("bad: ", n1, n2, row['rela'])
                    continue
                else:
                    r = Relationship(n2, row['rela'].upper(), n1, reltype='rxclass')
                    r['rel'] = row['rela']
                    print("good: ", n1, n2, index, r)
                    tx.merge(r)
                tx.commit()
                print('committed')

    show_finish(start)


if __name__ == '__main__':
    g = Graph(password="REDACTED")
    matcher = NodeMatcher(g)
    engine = create_engine('mysql+pymysql://root:REDACTED@localhost/umls')

    map_rxclasses(g, matcher, engine)
    label_nodes(g)
    build_umls_rels(g, matcher)

