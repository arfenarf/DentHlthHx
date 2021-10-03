// this dataset and some of these statements require a java heap size
// of at least 4G, twice what the default configuration offers.


CREATE CONSTRAINT  ON ( concept:Concept ) ASSERT concept.cui IS UNIQUE;
CREATE CONSTRAINT ON ( atom:Atom) ASSERT atom.aui IS UNIQUE;
CREATE CONSTRAINT ON ( patient:Patient ) ASSERT patient.patient IS UNIQUE;
CREATE CONSTRAINT ON ( pitem:PItem ) ASSERT pitem.pitem IS UNIQUE;
CREATE INDEX ON :Concept(is_core);
CREATE INDEX  ON :Atom(cui);
CREATE INDEX   ON :Atom(sourceCUI);
CREATE INDEX  ON :PItem(cui);
CREATE INDEX   ON :PItem(patient);
create index on :Concept(str);
create index on :Procedure(procedure);
create index on :PItem(page);

//create concepts
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///concepts.pipe' AS row FIELDTERMINATOR '|'

MERGE (c:Concept {cui: row.CUI})

ON CREATE SET
  c. str = row.STR,
  c.vocabulary = row.SAB,
  c.sourceCUI =  row.SCUI,
  c.termType =   row.TTY;


//create atoms (not using this july 2019)
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///atoms.pipe' AS row FIELDTERMINATOR '|'

MERGE (a:Atom{aui:row.AUI})
ON CREATE SET
  a.str = row.STR,
  a.cui = row.CUI,
  a.ispref = row.ISPREF,
  a.vocabulary = row.SAB,
  a.sourceCUI =  row.SCUI,
  a.termType =   row.TTY;

//link atoms to concepts (nor this)
MATCH (c:Concept)
WITH collect(DISTINCT c) AS cc
FOREACH(c IN cc|
   MERGE (a:Atom {cui:c.cui})
   CREATE (c)-[:IS_CONCEPT_FOR]->(a)
   CREATE (a)-[:REFERS_TO]->(c))


//add a label for concepts in the SNOMED core. Works fine as cypher.
// I created a table on the mysql server that can just be dumped to a useful location as a pipe
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///snomed_core.pipe' AS row FIELDTERMINATOR '|'

MERGE (c:Concept {cui: row.UMLS_CUI})
SET c.is_core = 'Y';


//add labels to concepts
//this kept crashing, moved the code to load_umls_graph.py
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///semtypes.pipe' AS row FIELDTERMINATOR '|'

MERGE (c:Concept {cui: row.CUI})
SET c.semtype = row.SEMTYPE
WITH c
CALL apoc.create.addLabels(c, [c.semtype]) YIELD node
REMOVE node.semtype
RETURN node;

//here's a big one, tying concepts together. It crashed repeatedly and I recommend
//doing this with load_umls_graph.py instead

USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///rels.pipe' AS row FIELDTERMINATOR '|'
MERGE (concept2:Concept {aui: row.AUI2})
MERGE (concept1:Concept {aui: row.AUI1})

WITH concept2, concept1, row
CALL apoc.create.relationship(concept2, row.RELA, {rel:row.REL}, concept1) YIELD rel
RETURN rel;

//patients
LOAD CSV WITH HEADERS FROM 'file:///patients.pipe' AS rows FIELDTERMINATOR '|'
WITH rows MERGE (p:Patient{patient:rows.Patient})
ON CREATE SET p.age = rows.AGE, p.sex = rows.SEX, p.chart = rows.CHART
ON MATCH SET  p.age = rows.AGE, p.sex = rows.SEX, p.chart = rows.CHART;


//procedures
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///procedures.pipe' AS rows FIELDTERMINATOR '|'
MERGE(pr:Procedure{procedure:rows.Procedure})
SET pr.Description =  rows.Description;

//patient to procedure detail - not loaded in v3
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///patient-procedure.pipe' AS rows FIELDTERMINATOR '|'
MATCH (pr:Procedure{code:rows.Procedure})
MATCH (pt:Patient{patient:rows.Patient})
WITH pr, pt, rows CREATE (pt)-[r:UNDERWENT {weight:rows.WEIGHT, reltype:'umsod'}]->(pr) ;

//roll up procedures to single weighted rel
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///patient-procedure-weighted.pipe' AS rows FIELDTERMINATOR '|'
MATCH (pr:Procedure{procedure:rows.Procedure})
MATCH (pt:Patient{patient:rows.PATIENT})
WITH pr, pt, rows CREATE (pt)-[r:UNDERWENT_WEIGHTED {weight:rows.WEIGHT, reltype:'umsod'}]->(pr) ;

//I don't think adding the pitem nodes to the graph adds value

//pt-to-pitem relationships.
//USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///patient-pitem.pipe' AS rows FIELDTERMINATOR '|'
//MERGE(pt:Patient{patient:rows.PATIENT})
//MERGE (pi:PItem{pitem:rows.NOTE_ID})
//WITH pt, pi, rows CREATE (pt)-[:REPORTS{reltype:'umsod'}]->(pi)
//SET pi.hx_type =  rows.HX_TYPE, pi.page = rows.PAGE, pi.date=rows.Date, pi.text=rows.TEXT;


//pitem to concept
//USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///pitem-concept.pipe' AS rows FIELDTERMINATOR '|'
//MERGE (pi:PItem{pitem:rows.NOTE_ID})
//MERGE (cui:Concept{cui:rows.CUI})
//WITH pi, cui MERGE (pi)-[:RELATED_TO {reltype:'umsod'}]->(cui);

//patient to (weighted) concept
USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///patient-concept.pipe' AS rows FIELDTERMINATOR '|'
MERGE (pt:Patient{patient:rows.PATIENT})
MERGE (cui:Concept{cui:rows.CUI})
WITH pt, cui, rows MERGE (pt)-[r:HX_WEIGHTED {reltype:'umsod'}]->(cui)
set r.weight = rows.WEIGHT;

//drop the reverse relationships because they're annoying
//this doesn't get all of them but most is good enough.
match (:Concept)-[r]-(:Concept)
where type(r) starts with 'HAS'
delete r;

match (:Concept)-[r]-(:Concept)
where type(r) starts with 'INVERSE'
delete r;


match ()-[r]-()
where not type(r) in ['HX_WEIGHTED', 'RELATED_TO', 'UNDERWENT_WEIGHTED','REPORTS' ]
set r.reltype = 'umls';

match ()-[r]-()
where type(r) in ['HX_WEIGHTED', 'RELATED_TO', 'UNDERWENT_WEIGHTED','REPORTS' ]
set r.reltype = 'umsod';

//These concepts are just plain useless, as they cover thousands upon thousands of patients
match (c:Concept{cui:'C0031831'})-[r]-(n) //"Medical Doctor"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0332307'})-[r]-(n) //"Type - Attribute"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0205164'})-[r]-(n) //"Major"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0439228'})-[r]-(n) //"Day"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0332183'})-[r]-(n) //"Frequent"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C1273517'})-[r]-(n) //"Used by"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C1515187'})-[r]-(n) //"Take"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0452428'})-[r]-(n) //"Drink"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0033213'})-[r]-(n) //"Problem"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0013227'})-[r]-(n) //"Drug or medicament"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0439234'})-[r]-(n) //"year"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C1524063'})-[r]-(n) //"Use of"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0332173'})-[r]-(n) //"Daily"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0521116'})-[r]-(n) //"Current"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0030705'})-[r]-(n) //"Patient"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C2587213'})-[r]-(n) //"Control"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0011900'})-[r]-(n) //"Diagnosis"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0700287'})-[r]-(n) //"Use"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C0040223'})-[r]-(n) //"Time"
where n:Patient or n:PItem
delete r;

match (c:Concept{cui:'C1280500'})-[r]-(n) //"Effect"
where n:Patient or n:PItem
delete r;

