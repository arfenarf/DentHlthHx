import datetime
import os
import re
from collections import Counter

import cx_Oracle
from sqlalchemy.engine import create_engine
import pandas as pd
import pandas.errors
from nltk.tokenize import sent_tokenize

# This version is tuned to optimize for only "explain" etc. entries with CLAMP

# These are parameters for what forms we will retrieve.
# We work on an assumption that we want the true state as of the last day of the retrieval.
# If you want to set a specific 'as-of' date, you'll have to write that in.

# remember to switch on metamap if you're going to use it
# home directory /Volumes/kgweber/public_mm
# ./bin/skrmedpostctl start
# ./bin/wsdserverctl start

PAGES_TO_PARSE = [1, 3]
pages_for_sql = ', '.join(str(x) for x in PAGES_TO_PARSE)

CLAMP_WRAPPER_OR_NATIVE = 'wrapper'  # 'wrapper if using the ohdsi wrapper, native if using just clamp'
RUN_CLAMP = False
PARSE_CLAMP_OUTPUT = False
CREATE_CORPUS = True

START_DATE = '2018-09-05'
END_DATE = '2018-09-07'

if CLAMP_WRAPPER_OR_NATIVE == 'native':
    TO_CLAMP_DIR = '/Volumes/kgweber/ClampMac_1.6.0/workspace/MyPipeline/health_hx_parsing/Data/Input'
    FROM_CLAMP_DIR = '/Volumes/kgweber/ClampMac_1.6.0/workspace/MyPipeline/health_hx_parsing/Data/Output'

else:
    # TO_CLAMP_DIR = '/Volumes/kgweber/ohdsi-NLPTools-master/clamp-wrapper/input'
    TO_CLAMP_DIR = 'Users/kgweber/tmp/input'
    # FROM_CLAMP_DIR = '/Volumes/kgweber/ohdsi-NLPTools-master/clamp-wrapper/output'

# BACKUP_DIR = '/Volumes/kgweber/temp_storage/backup'
# CORPUS_DIR = '/Volumes/kgweber/health_hx_corpus'
BACKUP_DIR = '/Users/kgweber/tmp/backup'
CORPUS_DIR = '/Users/kgweber/tmp/corpus'

dsn_tns = cx_Oracle.makedsn('REDACTED.umich.edu', '1521', service_name='REDACTED')
ax_con = cx_Oracle.connect(user=r'kgweber', password='REDACTED', dsn=dsn_tns)
cursor = ax_con.cursor()
tofino_engine = create_engine("mysql+mysqlconnector://admin:REDACTED@tofino.local/health_hx")

def find_long_repeats(txt: str, min_length=75, threshold=2):
    '''
    This function is designed to detect the repeated sentences about drugs that get pasted into
    our health history fields by students.

    :param txt: String to search for repeated sentences
    :param min_length: shortest sentence to be included in the filter. The idea is to avoid capturing common
    phrases like, "patient says."
    :param threshold: the minimum number of repeats that qualify a repeated sentence for filtering.
    :return:
    '''
    sents = sent_tokenize(txt)
    sent_counts = dict(Counter(sents))
    longs = {k: v for (k, v) in sent_counts.items() if v > threshold and len(k) > min_length}
    return longs


def clean_responses(df):
    print('cleaning responses')

    df = set_hx_type(df)

    # get rid of the significance to dentistry entries.
    df = df[~df['ITEM_LABEL'].str.contains('ignificance', regex=False)]

    # locate the remaining repeating text
    alltext = df.RESPONSE.str.cat(sep=' ')

    repeats = find_long_repeats(alltext)

    for repeat in repeats.keys():
        df.loc[:, 'RESPONSE'] = [re.sub(repeat, '', str(x)) for x in df['RESPONSE']]

    # get some searchable text.
    df.loc['process_text'] = df['RESPONSE'].str.strip()

    df.loc['process_text'] = df['process_text']

    # let's get the stupid enumeration information out of the text - the 1. 2. a. b. etc
    df.loc['process_text'] = df['process_text'].str.replace(r'^\S+\. +', '', regex=True)

    # clean out unnecessary nonprintables
    df.loc['process_text'] = df['process_text'].str.replace(r'[\r\n]', ' ', regex=True)  # line breaks
    df.loc['process_text'] = df['process_text'].str.replace(r'[^\x00-\x7f]', ' ', regex=True)
    df.loc['process_text'] = df['process_text'].str.rstrip('\x00')  # non-ascii

    df = df.loc[df['process_text'] != '']

    # we break up the df now so we can process each type of data independently if we want to.
    df_health = df.loc[df['HX_TYPE'] == 'medical']
    df_dental = df.loc[df['HX_TYPE'] == 'dental']
    df_labeled_med_list = df.loc[(df['HX_TYPE'] == 'medication') & (df['PARENT_LABEL'] != 'Other')]
    df_other_med_list = df.loc[(df['HX_TYPE'] == 'medication') & (df['PARENT_LABEL'].str.contains('Other'))]

    othermedlist = []

    # this block collapses the 'other' medication list data into a single row per 'other' item

    for name, group in df_other_med_list.groupby(['PForm', 'ParentFItem']):
        r = {'Patient': max(group['Patient']),
             'PForm': max(group['PForm']),
             'PItem': min(group['PItem']),
             'FormCode': max(group['FormCode']),
             'FORM_DATE': max(group['FORM_DATE']),
             'Page': max(group['Page']),
             'Row': min(group['Row']),
             'SubLevel': max(group['SubLevel']),
             'AnsType': 5,
             'FItem': None,
             'FITEM_GROUP': max(group['FITEM_GROUP']),
             'ParentFItem': max(group['ParentFItem']),
             'PARENT_LABEL': 'Other',
             'ITEM_LABEL': 'Merged Other Medications',
             'RESPONSE': None,
             'RNK': 1,
             'HX_TYPE': 'medication',
             'process_text': group['process_text'].str.cat(sep=' ')}
        othermedlist.append(r)

    df_other_med_list = pd.DataFrame(othermedlist)

    # now we glue it all back together
    export_df = df_health.append(df_dental, sort=False) \
        .append(df_labeled_med_list, sort=False).append(df_other_med_list, sort=False)

    # TODO Temporary Fix
    export_df['process_text'] = export_df['process_text'].str.slice(0, 256)

    export_df.reset_index(inplace=True)

    export_df['row_index'] = export_df.index

    export_df = export_df.sort_values(by='FORM_DATE').drop_duplicates('PItem', keep='last')

    print('writing safekeeping copy of responses')

    export_df.to_csv(BACKUP_DIR + '/uncoded_responses.csv', index=False)  # belt and suspenders

    print(str(len(export_df)) + ' cleaned rows')
    return (export_df)


def dump_to_clamp(df):
    # this sets you up to send the whole thing to clamp if you want to

    for index, row in df.iterrows():
        with open(os.path.join(TO_CLAMP_DIR, str(row.PItem) + '.txt'), 'w') as outfile:
            outfile.write(row.process_text)


def set_hx_type(df):
    df.loc[(df['Page'] == 1), 'HX_TYPE'] = 'medical'
    df.loc[df['Page'] == 2, 'HX_TYPE'] = 'dental'
    df.loc[(df['Page'] == 3), 'HX_TYPE'] = 'medication'

    return df


def look_up_umls_cuis(code_list):
    '''
    calls Ananke for UMLS CUIS
    :param code_list: Dataframe with columns 'VOCAB_CONCEPT_CODE' and 'TARGET_VOCABULARY_ID'
    :return: data frame of CUIs
    '''

    code_list = code_list.dropna()

    try:
        umls_cuis = pd.read_sql("""
           select * from KGWEBER.ANANKEV2
            where 
             CONCEPT_ID in {}
              
            """.format(tuple(code_list['TARGET_CONCEPT_ID'].values)), con=ax_con)
    except KeyError:
        pass

    return umls_cuis


def look_up_omop_concepts(concept_code: str, vocabulary: str):
    '''
    For the time being, this is running once per row in the source dataframe because I want there to be logic
    in split_cuis() that controls which value gets looked up.  There's probably a a better way to do this.
    :param concept_code: str code to look up
    :param vocabulary: str name of the vocabulary in the OMOP framework.
    :return:
    '''
    try:
        concept_id = str(cursor.execute(
            "select CONCEPT_ID from kgweber.concept c  where c.CONCEPT_CODE = '{}'and c.VOCABULARY_ID = '{}'"
                .format(concept_code, vocabulary)).fetchone()[0])

    except:
        concept_id = None

    return concept_id


def split_cuis(CUI):
    cui_dict = {'UMLS_CUI': None,
                'RXNORM_RXCUI': None,
                'RXNORM_GENERIC_RXCUI': None,
                'SNOMED_CT': None,
                'OMOP_CONCEPT': None}
    try:
        if 'SNOMED' in CUI:
            snomed_umls_split = CUI.split(' SNOMEDCT_US[')
            cui_dict['UMLS_CUI'] = snomed_umls_split[0]
            cui_dict['SNOMED_CT'] = re.findall(r'\d+', ','.join(snomed_umls_split[1:]))

        else:
            cuis = [x.strip() for x in CUI.split(',')]
            for cui in cuis:
                if cui[0] == 'C':
                    cui_dict['UMLS_CUI'] = cui
                if cui[0] == 'R':
                    cui_dict['RXNORM_RXCUI'] = re.search(r'\[(\w*)\]', cui).group(1)
                if cui[0] == 'G':
                    cui_dict['RXNORM_GENERIC_RXCUI'] = re.search(r'\[(\w*)\]', cui).group(1)
    except:
        pass

    # commenting this out because for now, I'm going to stick with UMLS concepts
    # oh, what the heck.

    if cui_dict['RXNORM_GENERIC_RXCUI'] is not None:
        cui_dict['OMOP_CONCEPT'] = look_up_omop_concepts(cui_dict['RXNORM_GENERIC_RXCUI'], 'RxNorm')

    elif cui_dict['RXNORM_RXCUI'] is not None:
        cui_dict['OMOP_CONCEPT'] = look_up_omop_concepts(cui_dict['RXNORM_RXCUI'], 'RxNorm')

    elif cui_dict['SNOMED_CT'] is not None:
        cui_dict['OMOP_CONCEPT'] = look_up_omop_concepts(cui_dict['SNOMED_CT'][0], 'SNOMED')

    b = pd.Series(cui_dict)

    return b


def load_clamp_output():
    # this is hardwired to the clamp wrapper output.  Reading clamp output itself is also in the
    # original version of this script

    clamp_in_df = pd.DataFrame()
    pitem_df = pd.DataFrame()

    if not os.path.exists(FROM_CLAMP_DIR + '/bad'):
        os.makedirs(FROM_CLAMP_DIR + '/bad')

    dirs = ['']
    for dir in dirs:
        thisdir = FROM_CLAMP_DIR + '/' + dir
        directory = os.fsencode(thisdir)

        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            print(filename)
            if filename.endswith(".txt"):
                try:
                    pitem_df = pd.read_csv(thisdir + filename, sep='\t', header=None)
                except pd.errors.EmptyDataError:
                    continue

                except pd.errors.ParserError:
                    move_to_bad(filename, thisdir)
                    continue

                if len(pitem_df) > 0:
                    if len(pitem_df.columns) == 11:
                        pitem_df['PItem'] = int(filename.split(sep='.')[0])
                        clamp_in_df = clamp_in_df.append(pitem_df)
                    else:
                        move_to_bad(filename, thisdir)
                        continue

    clamp_in_df.reset_index(inplace=True)

    clamp_in_df.columns = ['index', 'section_concept_id', 'snippet', 'offset', 'lexical_variant', 'nlp_system',
                           'note_nlp_merged_concept_ids', 'nlp_date', 'nlp_datetime', 'term_exists',
                           'term_temporal', 'term_modifiers', 'PItem']

    clamp_in_df[['term_temporal', 'term_modifiers']] = clamp_in_df[['term_temporal', 'term_modifiers']].fillna(value='')

    cui_df = clamp_in_df.note_nlp_merged_concept_ids.apply(split_cuis)

    # TODO there are better ways to do this now that we have the ANANKE table

    clamp_in_df['nlp_date'] = pd.to_datetime(clamp_in_df['nlp_date'])

    clamp_in_df['nlp_datetime'] = pd.to_datetime(clamp_in_df['nlp_datetime'])

    clamp_in_df = clamp_in_df.merge(cui_df[['UMLS_CUI', 'OMOP_CONCEPT']], left_index=True, right_index=True)

    clamp_in_df.rename(columns={'UMLS_CUI': 'note_nlp_source_concept_id', 'OMOP_CONCEPT': 'note_nlp_concept_id'},
                       inplace=True)

    clamp_in_df.drop('index', inplace=True, axis=1)

    clamp_in_df = clamp_in_df.dropna(subset=['note_nlp_source_concept_id']).drop_duplicates(
        ['PItem', 'note_nlp_source_concept_id'])

    return clamp_in_df


def move_to_bad(filename, thisdir):
    os.rename(thisdir + '/' + filename, FROM_CLAMP_DIR + '/bad/' + filename)


def fetch_starting_id(table, key):
    note_sequence_no = cursor.execute('select max({}) from {}'.format(key, table)).fetchone()
    if note_sequence_no[0] is None:
        new_id = 1
    else:
        new_id = note_sequence_no[0] + 1

    return new_id


# here's the main load and go
print('getting responses from database')

# start with a random sample of patients

patients = pd.read_sql("""
SELECT  *
FROM    (
        SELECT  distinct(t."Patient"), t."Procedure"
        FROM  axium.TRX t
        where t."TreatmentDate"> = to_date('2019-01-01', 'YYYY-MM-DD')
        and t."TreatmentDate" < = to_date('2019-12-31', 'YYYY-MM-DD')
        and t."Procedure" like 'D0150%'
        and t."Deleted" = 0
        ORDER BY
                dbms_random.value
        )
WHERE rownum <= 1000
""", con=ax_con, index_col=None)

# Parse the labeled headers

# Get the line items
# Join to a table mapping FITEM to OMOP Concept and Source Vocabulary Concept Codes
# Filter and join to a table mapping OMOP to UMLS

headers = pd.read_sql("""
    select fis.*,
            scm.TARGET_CONCEPT_ID, scm.TARGET_VOCABULARY_ID, scm.TARGETCONCEPTNAME as LEXICAL_VARIANT,
       CONCEPT.CONCEPT_CODE as vocab_concept_code
     from (
    select DISTINCT *
    from (select ap."Date",
                 pi."Patient",
                 pi."PForm",
                 pi."PItem",
                 fi."Page",
                 fi."Row",
                 pf."Date" as form_date,
                 fi."FItem",
                 fi."OrgFItem",
                 decode(fi."OrgFItem", 0, fi."FItem", fi."OrgFItem") as FITEM_GROUP,
                 pi."AppUser",
                 pfi."Text" as                                                                      parent_label,
                 fi."Text"  as                                                                      item_label,
                  TO_CHAR(pi."AnsYes") as                                                                      response,
                 rank() over (partition by pi."Patient", pi."FItem" order by pi."EntryDateTime" desc) rnk
          from
                   AXIUM.PFORM pf
                   left join AXIUM.PATIENT pt on pf."Patient" = pt."Patient"
                   left join AXIUM.PITEM pi on pf."PForm" = pi."PForm"
                   left join AXIUM.FITEM fi on pi."FItem" = fi."FItem"
                   left join AXIUM.FITEM pfi on pfi."FItem" = fi."ParentFItem"
                   left join AXIUM.APPOINT ap on ap."Patient" = pt."Patient"
    
          where pi."FormCode" in ('HEALTH')
            and ap."Date" >= to_date('{}', 'YYYY-MM-DD')
            and ap."Date" <= to_date('{}', 'YYYY-MM-DD')
            and pi."Date" <= to_date('{}', 'YYYY-MM-DD')
            and (fi."AnsType" = 0 and pi."AnsYes" = 1)
            and fi."FItem" not in (45138, 585765, 118823, 119491, 45115, 208146)
            and fi."OrgFItem" not in (45137, 45140, 208146)
            and fi."Page" in ({})
            and pf."Inactive" = 0
            and pi."Status" = 0
            and fi."Text" != 'Other'
    
          order by "Patient", "PForm", fi."Page", fi."Row") a
    
    where a.rnk = 1
      and a.response is not null
      order by "Patient", "PForm", "PItem") fis
    
      left join KGWEBER.SOD_FITEM_OMOP_MAP scm on to_char(fis.FITEM_GROUP) = scm.FITEM_GROUP
        left join KGWEBER.CONCEPT on scm.TARGET_CONCEPT_ID = CONCEPT_ID
     """.format(START_DATE, END_DATE, END_DATE, pages_for_sql), con=ax_con, index_col=None)

headers['TARGET_CONCEPT_ID'].fillna(0, inplace=True)
headers['TARGET_CONCEPT_ID'] = headers['TARGET_CONCEPT_ID'].astype(int)

header_match = headers[['TARGET_VOCABULARY_ID', 'VOCAB_CONCEPT_CODE', 'TARGET_CONCEPT_ID']].drop_duplicates()
headers = set_hx_type(headers)

# this  makes it possible to go get some of the UMLS CUIs. I'm not sure which way we'll land.  For now, I think I'm
# a little more looking at this from the POV of UMLS.
header_cuis = look_up_umls_cuis(header_match)

matched_headers = pd.merge(headers, header_cuis[['CONCEPT_ID', 'CUI']], how='left', left_on=['TARGET_CONCEPT_ID'],
                           right_on=['CONCEPT_ID'], sort=False)

# Parse the free text

raw_sql = pd.read_sql("""
    select DISTINCT *
    from (select -- ap."Date",
                 pi."Patient",
                 pi."PForm",
                 pi."PItem",
                 pi."FormCode",
                 pf."Date" as form_date,
                 fi."Page",
                 fi."Row",
                 fi."SubLevel",
                 fi."AnsType",
                 fi."FItem",
                 decode(fi."OrgFItem", 0, fi."FItem", fi."OrgFItem") as FITEM_GROUP,
                 fi."ParentFItem",
                 pi."AppUser",
                 pfi."Text" as                                                                      parent_label,
                 fi."Text"  as                                                                      item_label,
                 pi."AnsText"  as                                                                      response,
                 rank() over (partition by pi."Patient", pi."FItem" order by pi."EntryDateTime" desc) rnk
          from
                   AXIUM.PFORM pf
                   left join AXIUM.PATIENT pt on pf."Patient" = pt."Patient"
                   left join AXIUM.PITEM pi on pf."PForm" = pi."PForm"
                   left join AXIUM.FITEM fi on pi."FItem" = fi."FItem"
                   left join AXIUM.FITEM pfi on pfi."FItem" = fi."ParentFItem"
                   left join AXIUM.APPOINT ap on ap."Patient" = pt."Patient"
    
          where pi."FormCode" in ('HEALTH')
            and ap."Date" >= to_date('{}', 'YYYY-MM-DD')
            and ap."Date" <= to_date('{}', 'YYYY-MM-DD')
            and pi."Date" <= to_date('{}', 'YYYY-MM-DD')
            and fi."AnsType" = 5
            and fi."FItem" not in (45138, 585765, 118823, 119491, 45115)
            and fi."OrgFItem" not in (45137, 45140) 
            and fi."Page" in ({})
            and pf."Inactive" = 0
            and pi."Status" = 0
    
          order by "Patient", "PForm", fi."Page", fi."Row") a
    
    where a.rnk = 1
      and a.response is not null
      order by "Patient", "PForm", "Page", "Row"
     """.format(START_DATE, END_DATE, END_DATE, pages_for_sql), con=ax_con, index_col=None)

print('cleaning responses')
print(str(len(raw_sql)) + ' raw rows')

responses = clean_responses(raw_sql)
responses.to_sql('hx_text', con = tofino_engine, index = False, if_exists='replace', chunksize=100)
# Oh, you say you want this to go to to CLAMP?
# the code requires that you use debug and pause until we have an automated way to call it..


print('dumping clamp')
# this sets up files to send to CLAMP.

if RUN_CLAMP:
    dump_files = responses[responses['Page'].isin(PAGES_TO_PARSE)]
    dump_to_clamp(dump_files)

# there's a whole song and dance we have to do to get files over to CLAMP and get them processed.
# which isn't in here
# this assumes we've got 'em and are reading them back

if CREATE_CORPUS:

    corpus_labels = matched_headers[['PForm', 'PItem', 'Page', 'Row', 'ITEM_LABEL']].fillna('')
    corpus_labels['Text'] = corpus_labels['PItem'].astype(str) + " | " + corpus_labels['ITEM_LABEL']
    corpus_text = responses[['PForm', 'PItem', 'Page', 'Row', 'ITEM_LABEL', 'process_text']]
    corpus_text['Text'] = corpus_text['PItem'].astype(str) + " | " + corpus_text['ITEM_LABEL'] + " | " + corpus_text[
        'process_text']

    corpus = pd.concat([corpus_text[['PForm', 'Page', 'Row', 'Text']],
                        corpus_labels[['PForm', 'Page', 'Row', 'Text']]]).drop_duplicates()
    corpus.sort_values(['PForm', 'Page', 'Row'], inplace=True)

    corpus_group = corpus[['PForm', 'Text']].groupby('PForm').agg(Export_Text=('Text', '\n'.join)).reset_index()

    for index, row in corpus_group.iterrows():
        with open(os.path.join(CORPUS_DIR, str(row.PForm) + '.txt'), 'w') as outfile:
            outfile.write(row.Export_Text)

    print('blaf')

if PARSE_CLAMP_OUTPUT:  # short-circuit for the sake of debugging expediency
    clamp_df = load_clamp_output()

    clamp_df.to_csv(BACKUP_DIR + '/clamp_parsed.csv', index=False)

else:
    clamp_df = pd.read_csv(BACKUP_DIR + '/clamp_parsed.csv')

## build the tables that will populate Note

note_data_headers = matched_headers[['Patient', 'FORM_DATE', 'HX_TYPE', 'ITEM_LABEL', 'AppUser', 'PForm', 'FItem']]
note_data_responses = responses[['Patient', 'FORM_DATE', 'HX_TYPE', 'PARENT_LABEL', 'AppUser', 'PForm', 'ParentFItem']]
note_data_responses = note_data_responses.rename(columns={'PARENT_LABEL': 'ITEM_LABEL', 'ParentFItem': 'FItem'})

# stick them together
note_data = pd.concat([note_data_headers, note_data_responses], axis=0, sort=False)

note_data['AppUser'] = note_data['AppUser'].fillna(0).astype(int)

# these are the keys we're going to use to glue stuff together
note_data = note_data.drop_duplicates(['Patient', 'PForm', 'FItem'])
note_data['note_source_value'] = note_data['PForm'].astype(str) + '-' + note_data['FItem'].astype(str)
note_data = note_data.rename(
    columns={'Patient': 'person_id', 'FORM_DATE': 'note_date', 'ITEM_LABEL': 'note_title', 'AppUser': 'provider_id'})
note_data = note_data.sort_values('person_id')

## build the tables that will populate NoteNLP

# bring back the NLP results into the responses

note_nlp_responses = responses[['Patient', 'PForm', 'ParentFItem', 'PItem', 'process_text']].merge(clamp_df,
                                                                                                   how='inner',
                                                                                                   on='PItem')
note_nlp_responses = note_nlp_responses.drop_duplicates()
note_nlp_responses.rename(columns={'ParentFItem': 'FItem'}, inplace=True)
note_nlp_responses['section_concept_id'] = 0  # separating the txt from the labels for now

# get the header cui items
note_nlp_headers = matched_headers[
    ['Patient', 'PForm', 'FItem', 'ITEM_LABEL', 'CUI', 'TARGET_CONCEPT_ID', 'LEXICAL_VARIANT']].drop_duplicates()
note_nlp_headers.rename(columns={'ITEM_LABEL': 'process_text', 'CUI': 'note_nlp_source_concept_id',
                                 'TARGET_CONCEPT_ID': 'note_nlp_concept_id', 'LEXICAL_VARIANT': 'lexical_variant'},
                        inplace=True)
note_nlp_headers['section_concept_id'] = 1
note_nlp_headers['offset'] = None
note_nlp_headers['nlp_system'] = 'SoD Usagi Mapping'
note_nlp_headers['nlp_date'] = datetime.date.today()
note_nlp_headers['nlp_datetime'] = datetime.datetime.today()
note_nlp_headers['term_exists'] = True
note_nlp_headers['term_temporal'] = None
note_nlp_headers['term_modifiers'] = None

# stick them together
note_nlp_data = pd.concat([note_nlp_headers, note_nlp_responses], axis=0, sort=False).sort_values(['Patient', 'FItem'])
note_nlp_data = note_nlp_data.rename(columns={'Patient': 'person_id'})

# drop the rows that have no useful umls or omop concdept
note_nlp_data = note_nlp_data.loc[
    (note_nlp_data.note_nlp_concept_id != 0) | (~note_nlp_data.note_nlp_source_concept_id.isna())]

# clean up dates
note_nlp_data['nlp_date'] = pd.to_datetime(note_nlp_data.nlp_date).dt.date

# truncate some text
note_nlp_data['snippet'] = note_nlp_data['snippet'].str[0:250]
note_nlp_data['lexical_variant'] = note_nlp_data['lexical_variant'].str[0:250]
note_nlp_data['term_temporal'] = note_nlp_data['term_temporal'].str[0:50]
note_nlp_data['term_modifiers'] = note_nlp_data['term_modifiers'].str[0:2000]

# clean up Nonetypes
note_nlp_data['note_nlp_concept_id'] = note_nlp_data['note_nlp_concept_id'].fillna(value=0)
note_nlp_data[['note_nlp_source_concept_id', 'term_temporal', 'term_modifiers', 'snippet', 'lexical_variant']] = \
    note_nlp_data[
        ['note_nlp_source_concept_id', 'term_temporal', 'term_modifiers', 'snippet', 'lexical_variant']].fillna(
        value='')

# build the tables with SQL
# This will need better code to support refreshing someday. Right now, is very naive

# I still want to do this with SQLAlchem ORM but am just having fits. So this is the brute-force way

note_data['written_to_db'] = None
note_nlp_data['written_to_db'] = None

note_key = fetch_starting_id('kgweber.sod_note', 'note_id')
note_nlp_key = fetch_starting_id('kgweber.sod_note_nlp', 'note_nlp_id')

ax_con = cx_Oracle.connect(user=r'kgweber', password='hamish1rupert', dsn=dsn_tns)
cursor = ax_con.cursor()
ax_con.autocommit = True

for index, row in note_data.iterrows():
    if row.HX_TYPE == 'medical':
        note_type_concept_id = 706577
    if row.HX_TYPE == 'medication':
        note_type_concept_id = 42527645
    else:
        note_type_concept_id == None

    statement = """
                    INSERT into KGWEBER.SOD_NOTE (note_id, person_id, note_date, note_type_concept_id, note_title,
                    encoding_concept_id, language_concept_id, provider_id, note_source_value) 
                    VALUES (:nid, :pid, :ndate, :ncid, :nt, :necid, :lcid, :prid, :nsv)
                   """

    values = {'nid': note_key, 'pid': row.person_id, 'ndate': row.note_date, 'ncid': note_type_concept_id,
              'nt': row.note_title, 'necid': 32678, 'lcid': 4180186, 'prid': row.provider_id,
              'nsv': row.note_source_value}

    cursor.execute(statement, values)

    nlp_items = note_nlp_data.loc[(note_nlp_data.PForm == row.PForm) & (note_nlp_data.FItem == row.FItem),]

    if len(nlp_items) > 0:
        for nlp_index, nlp_row in nlp_items.iterrows():
            statement = """
                    INSERT into KGWEBER.SOD_NOTE_NLP (
                    note_nlp_id, 
                    note_id,
                    section_concept_id,
                    snippet, offst,
                    lexical_variant,
                    note_nlp_concept_id,
                    nlp_system,
                    nlp_date,
                    term_exists,
                    term_temporal,
                    term_modifiers,
                    note_nlp_source_concept_id
                    )
                    VALUES (
                    :nnid,
                    :nid,
                    :scid,
                    :snip,
                    :offst,
                    :lex,
                    :nncid,
                    :syst,
                    :nlpd,
                    :te,
                    :tt,
                    :tm,
                    :nnscid
                    )
                   """

            values = {
                'nnid': note_nlp_key,
                'nid': note_key,
                'scid': nlp_row.section_concept_id,
                'snip': nlp_row.snippet,
                'offst': nlp_row.offset,
                'lex': nlp_row.lexical_variant,
                'nncid': int(nlp_row.note_nlp_concept_id),
                'syst': nlp_row.nlp_system,
                'nlpd': nlp_row.nlp_date,
                'te': nlp_row.term_exists,
                'tt': nlp_row.term_temporal,
                'tm': nlp_row.term_modifiers,
                'nnscid': nlp_row.note_nlp_source_concept_id
            }

            print(values)

            cursor.execute(statement, values)

            note_nlp_key += 1

    note_key += 1

    ax_con.commit()

ax_con.close()

print('hold here')
