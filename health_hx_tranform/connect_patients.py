# this code connects patients directly together based on their health history details.
# It replaces what we'd hope to see, a projection through the Neo4j tooling, but the laptop
# just can't cope with that right now.

# 2021 update this all is superceded by recent Neo4j upgrades, needs to be rebuilt

import datetime

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from igraph import Graph as iGraph
from py2neo import Graph as pGraph, NodeMatcher

from UMUtils.filters import compute_significance, prune


def start_timer():
    start = datetime.datetime.now()
    print(start)
    return start


def show_finish(start_time):
    finish = datetime.datetime.now()
    print(finish)
    print(finish - start_time)


def build_pt_pt_rels(graph):
    # this is not at all a trivial process.  It speeds up as it goes along b/c each new patient may already
    # be connected to other patients. But don't start this 10 minutes before you want to do a presentation.
    # ask me know I know this.  1000 patients take about 20 minutes

    start = start_timer()

    # for the time being, only tie together health hx, not treatment hx
    pts = graph.run(
        "match (pt:Patient)-[:UNDERWENT_WEIGHTED]-(pr:Procedure) where (pr.procedure starts with 'D47' or pr.procedure starts with 'D48') return pt").to_data_frame()
    # pts =graph.run("match (pt:Patient)-[:UNDERWENT_WEIGHTED]-(pr:Procedure) return pt").to_data_frame()

    for index, node in pts.iterrows():
        pt = node[0]['patient']

        # b = graph.run("""
        #     match (p1:Patient{{patient:'{}'}})-[h1:HX_WEIGHTED]->(:Concept)<-[h2:HX_WEIGHTED]-(p2:Patient)-[:UNDERWENT_WEIGHTED]-(pr:Procedure)
        #     where not (p1)-[:PT_REL]-(p2)
        #     and (pr.procedure starts with 'D47' or pr.procedure starts with 'D48')
        #     with p1, p2, sum(toInteger(h1.weight)) + sum(toInteger(h2.weight)) as hx_weight
        #     match (p1)-[u1:UNDERWENT_WEIGHTED]->(:Procedure)<-[u2:UNDERWENT_WEIGHTED]-(p2)
        #     with p1, p2, hx_weight, sum(toInteger(u1.weight)) + sum(toInteger(u2.weight)) as px_weight
        #     merge (p1)-[:PT_REL{{hx_weight:hx_weight, px_weight:px_weight}}]-(p2)
        #     """.format(pt))

        b = graph.run("""
            match (p1:Patient{{patient:'{}'}})-[h1:HX_WEIGHTED]->(:Concept)<-[h2:HX_WEIGHTED]-(p2:Patient)-[:UNDERWENT_WEIGHTED]-(pr:Procedure)
            where not (p1)-[:PT_REL]-(p2)
            and (pr.procedure starts with 'D47' or pr.procedure starts with 'D48')
            with p1, p2, sum(toInteger(h1.weight)) + sum(toInteger(h2.weight)) as hx_weight
            merge (p1)-[:PT_REL{{hx_weight:hx_weight}}]-(p2)
            """.format(pt))

        print('index: {} patient: {} relationships added: {}'.format(index, pt, b.stats()['relationships_created']))

    show_finish(start)


def subgraph_igraph(pg, pct):
    query = """
    MATCH (p1:Patient)-[r:PT_REL]-(p2:Patient)
    RETURN p1.patient, p2.patient, r.hx_weight AS weight
    """

    ig = iGraph.TupleList(pg.run(query), weights=True)
    compute_significance(ig)
    pruned = prune(ig, percent=pct)
    return pruned


if __name__ == '__main__':

    REWIRE_PATIENTS = False

    DO_COMMUNITY_DETECTION = False

    pg = pGraph(password="REDACTED")

    # if you want to build the patient rels GOD ALMIGHTY THIS IS SLOW
    # about an hour for 1600 pts

    if REWIRE_PATIENTS:
        build_pt_pt_rels(pg)

    if DO_COMMUNITY_DETECTION:
    # cutting this guy down hard.
        pruned = subgraph_igraph(pg, 10)

        pruned.summary()
        pruned.write('perio analysis/pruned_perio.gml')

        pruned = iGraph.Read_GML('perio analysis/pruned_perio.gml')

        # so let's do some community identification
        coms = pruned.community_leading_eigenvector(clusters=8, weights='weight')
        pruned.vs['community'] = coms.membership

        pruned.write('perio analysis/pruned_perio_leading_eigenvector.gml')

        # produces a huge dendogram that isn't terribly helpful
        # also very slow. I may be doing it wrong.
        # coms_dendo = pruned.community_edge_betweenness(clusters=8, directed=False, weights='weight')
        # blaf = coms_dendo.as_clustering()
        # blaf.sizes()
        # # [876, 3, 1, 1, 2, 1, 2, 1]
        # import pickle
        #
        # filename = 'perio analysis/pruned_perio_edge_betweenness.pkl'
        # outfile = open(filename, 'wb')
        # pickle.dump(coms_dendo, outfile)
        # outfile.close()

        # write it back to the graph

        nm = NodeMatcher(pg)
        pg.run('match (p:Patient) set p.community = Null')
        for ignode in pruned.vs:
            pgnode = nm.match('Patient', patient=ignode['name']).first()
            pgnode['community'] = ignode['community']
            pg.push(pgnode)

    # get a df of germane communities and their concepts

    query = """
    match (c:Concept)--(p:Patient)-[r:PT_REL]-(:Patient)
    where p.community IS NOT NULL
    with  distinct p, c
    return p.community as community,count(p) as pt_ct, c.cui as cui, c.str as concept_label, labels(c) as semtype
    ORDER BY community, pt_ct desc
    """

    communities_df = pg.run(query).to_data_frame()
    communities_df = communities_df.loc[communities_df['pt_ct'] > 1]

    # get a df of random pts and their concepts

    query = """
    match (c:Concept)--(p:Patient)
    where toString(p.patient) ends with '2'
    with  distinct p, c
    return c.str as concept_label,c.cui as cui,  count(p) as pt_ct
    ORDER BY pt_ct desc;"""

    allpts_df = pg.run(query).to_data_frame()
    allpts_df = allpts_df.loc[allpts_df['pt_ct'] > 1]
    allpts_df['pct_of_total'] = allpts_df['pt_ct'] / allpts_df['pt_ct'].sum()

    # This gets you the ability to paste into the "Raw Data" tab in the Perio-Patient-Concepts Workbook
    # Remember to make sure that you pull existing rows so there's no confusion in the overlap with old data
    # Of course, there's a TODO to get away from inelegant Excel

    # communities_df.to_clipboard(index=False)

    # BUT INSTEAD, let's do this:

    # make a lookup table
    concept_lookup = communities_df.groupby(['cui', 'concept_label']).count()
    concept_lookup.reset_index(inplace=True)

    # pivot and add back the concept labels
    concept_community_df = communities_df.pivot(index='cui', columns='community', values='pt_ct')
    concept_community_df = concept_community_df.merge(concept_lookup[['cui', 'concept_label']], on='cui', how='left')

    concept_community_df['count_totals'] = concept_community_df.sum(axis=1)
    concept_community_df['overall_pct_of_total'] = concept_community_df['count_totals'] / concept_community_df[
        'count_totals'].sum()

    outlier_df = pd.DataFrame(columns = ['concept_label', 'dist_from_mean', 'community', 'overall_pct_of_total', 'count_totals'])

    for i in range(0, 4):
        newcolname = str(i) + '_pct_total'
        concept_community_df[newcolname] = concept_community_df[i] / concept_community_df[i].sum()
        distname = str(i) + '_dist_from_mean'
        concept_community_df[distname] = concept_community_df[newcolname] - concept_community_df['overall_pct_of_total']

        # this bit locates every row in the df where the community's distance from the mean is more than
        # two standard deviations away.

        bloink =concept_community_df.loc[
                  abs(concept_community_df[str(i) + '_dist_from_mean']) >
                    2 * concept_community_df['overall_pct_of_total'].std(skipna=True),
                  ['concept_label', distname, 'overall_pct_of_total', 'count_totals']]

        bloink['community'] = str(i)
        bloink.rename(columns = {distname: 'dist_from_mean'}, inplace=True)

        outlier_df = outlier_df.append(bloink, sort= False).sort_values('concept_label')

    # I built this expecting to see a difference between the perio pts and the overall population but it isn't showing
    # up. Even when I pulled one standard deviation out, they just vanished.
    #
    # Reconsidering the whole methodology right now.

    # outlier_concepts_all_pts = allpts_df.loc[allpts_df['concept_label'].isin(outlier_df['concept_label'])].sort_values(
    #     'concept_label')
    # outlier_concepts_all_pts = outlier_concepts_all_pts.merge(
    #     concept_community_df[['concept_label', 'overall_pct_of_total']],
    #     on='concept_label', how='left')
    # outlier_concepts_all_pts['dist_from_mean'] = outlier_concepts_all_pts['pct_of_total'] - outlier_concepts_all_pts[
    #     'overall_pct_of_total']
    # outlier_concepts_all_pts['community'] = 'SOD All'
    #
    # outlier_concepts_all_pts = outlier_concepts_all_pts.loc[
    #     abs(outlier_concepts_all_pts['dist_from_mean']) >
    #     1 * outlier_concepts_all_pts['overall_pct_of_total'].std(skipna=True)]
    #
    # outlier_df = outlier_df.append(
    #     outlier_concepts_all_pts[['concept_label', 'dist_from_mean', 'community', 'overall_pct_of_total']])

    color_map = pd.DataFrame({'community': ['0', '1', '2', '3'], 'color': ['green (210)', 'purple (293)', 'blue (154)', 'orange (149)']})

    outlier_df = outlier_df.merge(color_map, on = 'community', how='left')

    outlier_df.concept_label = outlier_df['concept_label'] + ' (' + outlier_df['count_totals'].astype(int).astype(str) + ')'

    plot_df = outlier_df[['color', 'concept_label', 'dist_from_mean']].pivot('concept_label', 'color', 'dist_from_mean')

    plt.subplots(figsize = (10, 20))
    sns.heatmap(plot_df, center = 0,  cmap="RdYlBu_r")
    plt.show()


