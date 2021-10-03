# How to Build

Notes, KGW 2021:

This is a rough set of notes to self describing how these proof-of-concept parts come together. The initial work was done summer 2019 - the COVID pandemic has forced the whole project into standby until now. I am at this moment getting the work back off the ground and into motion. Technology has moved on since then and this whole process requires two major changes:
* First, I need to update the NLP tooling to a more modern approach and then build a trial to determine whether SpaCy, MetaMap, or CLAMP is the best tool for this job, and if so, what tuning each will need to go from research-type work into production-caliber.
* Apply SNOMED-CT to this mapping work in order to support the tuning with an ontology of health information. I would like to feed each found concept back into SNOMED for a sanity check to see whether the concept is a near-ish neighbour of the parent item in the health hx form, and to prefer near neighbours when multiples are located.
* Pull in treatment notes as well as the health hx/problem list data
* Devise methods for getting clinical validation of this information

## Current approach
The overall process right now is:

In `map_hx_text_v2.py`:

* Extract data from the EHR and transform the patient health history tables from their current structure into short document-grain data suitable for parsing
* Apply clinically-validated labels to each of the checkbox items
* Run one or more NLP tools (currently MetaMap and/or CLAMP) over the dataset
* Write the data in a preliminary Neo4j load form

in `load_umls_graph.py`:
* bring the text files into the Neo4j graph datab ase

in `connect_patients.py`: 
* This is effectively a manual projection of the heterogeneous graph of patients, providers, procedures, and history-bits into a homogeneous graph of patients who are linked to each other with edges weighted by the number of health-history issues they have in common.  
* The next generation of this work will be to let the Neo4j database do the projection, but this functionality wasn't available when I was first drafting this approach.

How to build the data set from the ground up.

Notes as I go along:  Metamap only for now, maybe move to CLAMP now that
I have it available on my own machine. V3 needs to go to SpaCy

Check to be sure the metamap processes are running

Check the start/end dates hardcoded in map_hx_text.py

decide whether you're going to append to the X_PARSED tables. If not, truncate them.

TODO - start writing to database every 1000 records or so. Upside: Safety and ability
to start over if I need to. Downside: requires live db connection for the duration of the
run.

Run `map_hx_text.py`, setting the database coordinates to the server you want to use. Last round assumes
that we're running over Mident Prod and writing temp tables to the KGWEBER schema.
Extracting about two months of patients takes [] hours. The additional
time per month is not linear, because recalled patients won't be scanned twice.

TODO - improve section header coding

TODO - clean up the process so duplicate rows get removed. Try to avoid dupes anyway

TODO - migrate to SpaCy/Bert

TODO - build code so it checks with the existing database and only collects patients
new to the system so we can do an incremental load

In order to merge all this together between Neo4j and the Axium database:  We'll have to build
a parent script that's blending the various components into a single action. I think this work
is best taken over to Airflow after QA over the one-off process is finished.

SO: the end of `map_hx_text.py` has to be run by hand, as does much of the rest of that mess still.
Truncate the existing `METAMAP_PARSED` and load manually from the file in the backup dir (`metamap_parsed.csv`).
Also truncate `RESPONSES` and load with `uncoded_responses.csv`

Now, build the text files that will create the Neo4J entries. Is it theoretically possible to do this with
a direct database connection? Probably.  Does attempting this bring my machine to its knees? Absolutely.

Open up `extracting_umls_queries_mysql.sql` and run it in the Oracle console for any tables you need to 
replace. Stuff the tables somewhere convenient (I've been using /Volumes/kgweber/UMLS_NEO4J_LOAD). When you
have all the pipe files you want, move them to the `import` directory that belongs to your Neo4j instance.

You can start to build a fresh database from the beginning with a clean UMLS build, but it takes **forever**.
Doing it in pure Cypher is a mess, so much of this work has moved to `load_umls_graph.py`. 
This time, I just deleted the patients and Pitems and all relationships between them and rebuild those things over
the existing UMLS nodes and rels.

Refreshing patient data means replacing and updating these files:

patient-concept.pipe
patient-pitem.pipe
patient-procedure-weighted.pipe
patient-procedure.pipe
patients.pipe
pitem-concept.pipe
procedures.pipe

`build_umls_neo4j.cypher` has all the bits you need to do this. Pay attention to comments that direct you to 
use `load_umls_graph.py` instead.

Now, in order to do our networking, we need to build a meta-layer connecting patient to patient. In theory,
you can do this projection with virtual relationships in Neo4j. In reality, the laptop can't handle it, so
we're doing it more gently via a script: `connect_patients.py`