import uuid
from itertools import product

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import squarify
from py2neo import Graph
import py4cytoscape as p4c

def clear_graph(g):
    g.run('match (n)-[r]-() delete r delete n')
    g.run('match(n) delete n')


def load_excel(yrs):
    whole_node_df = pd.DataFrame()
    whole_edge_df = pd.DataFrame()

    for yr in yrs:
        filename = f'rotation_analysis_{yr}.xlsx'
        swap_sheetname = f'Swaps {yr}'
        student_sheetname = f'Uniq Lookup {yr}'
        swaps = pd.read_excel(filename, sheet_name=swap_sheetname, usecols="F:L").drop(
            columns=['stud_firstlast']).dropna()
        swaps.approved = pd.to_datetime(swaps.approved, errors='coerce')
        swaps = swaps.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Each swap that didn't require a substitute got a swap_uniq of "nsn." We have to
        # make a distinct node for each of them.

        swaps.reset_index(inplace=True)
        swaps.loc[swaps['swap_uniq'] == 'nsn', 'swap_uniq'] = 'nsn' + yr + swaps['index'].astype('string')
        swaps['school_year'] = yr[0:2] + "-" + yr[2:4]
        nsns = pd.DataFrame(columns=['CLASS', 'UNIQ'])
        nsns.UNIQ = swaps.loc[swaps['swap_uniq'].str.startswith('nsn'), 'swap_uniq']
        nsns.CLASS = 0

        students = pd.read_excel(filename, sheet_name=student_sheetname, usecols='A:F').drop(
            columns=['FL', 'LAST', 'FIRST'])
        students = pd.concat([students, nsns], axis=0)
        students['gradclass'] = np.where(students['CLASS'] == 0, 0,
                                         2000 + 4 - students['CLASS'] + int(yr[2:4]))
        students.drop('CLASS', axis=1, inplace=True)
        students.UNIQ = students.UNIQ.str.strip()



        whole_node_df = pd.concat([whole_node_df, students], ignore_index=True)
        whole_edge_df = pd.concat([whole_edge_df, swaps], ignore_index=True)

    whole_node_df = whole_node_df.drop_duplicates(['UNIQ'])
    whole_node_df['rand_id'] = [uuid.uuid4() for _ in range(len(whole_node_df.index))]

    whole_edge_df['month'] = pd.DatetimeIndex(whole_edge_df.approved).month
    whole_edge_df['year'] = pd.DatetimeIndex(whole_edge_df.approved).year
    whole_edge_df['sy_month'] = np.where(whole_edge_df.month < 5, whole_edge_df.month + 8, whole_edge_df.month - 4)
    whole_edge_df['approved'].fillna(method='backfill', inplace=True)

    whole_edge_df['edge_call'] = ("match (from{uniq:'" + whole_edge_df['student_uniq'] +
                                  "'}) match (to {uniq:'" + whole_edge_df['swap_uniq'] +
                                  "' }) merge (from)-[r:swapped_with {rotation:'" + whole_edge_df['rotation'] +
                                  "', change:'" + whole_edge_df['change'] + "', reason:'" + whole_edge_df['reason'] +
                                  "', approved:'" + whole_edge_df['approved'].dt.strftime('%Y-%m-%d') +
                                  "', school_year:'" + whole_edge_df['school_year'] + "'}]->(to)")
    return (whole_node_df, whole_edge_df)


def load_graph(g, n, e, wipe_graph=False):
    if wipe_graph:
        clear_graph(g)

    for index, row in n.iterrows():
        if row['gradclass'] == 0:
            label = 'NoSwap'
        else:
            label = 'Student'

        g.run(f"""
        CREATE (a:{label} {{uniq:'{row['UNIQ']}', gradclass:'{row['gradclass']}',rand_id:'{row['rand_id']}',
         itdp:' {row['ITDP  ']}'}})
        """)

    for index, row in e.iterrows():
        g.run(row['edge_call'])


def load_catalogue(g, sy, cl, mod):
    cur = g.run("call gds.graph.list() YIELD graphName, degreeDistribution;")
    extant = cur.to_data_frame()
    if len(extant) > 0:
        # wipe the graphs "call gds.graph.drop('{gr}');"
        for index, row in extant.iterrows():
            g.run(f"call gds.graph.drop('{row['graphName']}')")

    sgs = pd.DataFrame(list(product(schoolyears, classes, modes)), columns=['schoolyears', 'classes', 'modes'])
    sgs['startyear'] = 2000 + sgs['schoolyears'].str[0:2].astype('int')
    sgs['endyear'] = sgs['startyear'] + 1
    sgs['numclass'] = sgs['classes'].str[1:2].astype('int')
    sgs['gradclass'] = (sgs['endyear'] + 4 - sgs['numclass']).astype('str')
    sgs['graphname'] = sgs['schoolyears'] + "-" + sgs['classes'] + "-" + sgs['modes']
    sgs['nodequery'] = np.where(sgs['modes'] == 'all',
                                ("match (s:Student{gradclass:\"" + sgs['gradclass'] +
                                 "\"}) optional match (s)-[r]-(t) with collect (id(s)) + collect(id(t)) as foo unwind foo as f return distinct f as id"),
                                ("match (s:Student{gradclass:\"" + sgs['gradclass'] +
                                 "\"}) optional match (s)-[r]-(t:Student) with collect (id(s)) + collect(id(t)) as foo unwind foo as f return distinct f as id")
                                )
    sgs['edgequery'] = np.where(sgs['modes'] == 'all',
                                ("match (s:Student)-[r]-(t) where s.gradclass = \"" + sgs['gradclass'] +
                                 "\" and r.school_year = \"" + sgs['schoolyears'] +
                                 "\" return id(s) as source, id(t) as target"),
                                ("match (s:Student)-[r]-(t:Student) where s.gradclass = \"" + sgs['gradclass'] +
                                 "\" and r.school_year = \"" + sgs['schoolyears'] +
                                 "\" return id(s) as source, id(t) as target")
                                )

    for index, row in sgs.iterrows():
        g.run("call gds.graph.create.cypher('" + row['graphname'] + "', '" + row['nodequery'] + "', '" + row[
            'edgequery'] +
              "', {parameters: {relationshipValidation:false}})")


def get_subgraph_stats(g):
    resp = g.run("call gds.graph.list() YIELD graphName, nodeCount, relationshipCount, density, degreeDistribution")
    degree_df = resp.to_data_frame()
    dd = pd.json_normalize(degree_df.degreeDistribution)
    degree_df = pd.concat([degree_df, dd], axis=1).drop(columns=['degreeDistribution'])

    return degree_df


def run_algos(g, sgs, algos):

    for sg in sgs.graphName:
        for a in algos:
            g.run(f"CALL gds.{a}.write('{sg}', {{writeProperty: '{a}-{sg}'}})")


def plot_plots(g, edg, degree_df):
    # histogram of changes by class/year
    p = sns.FacetGrid(degree_df, row='class', col='school_year')
    p.map(sns.histplot, 'value', binwidth=1)
    plt.subplots_adjust(top=0.9)
    p.fig.suptitle('Distribution of Number of Exchanges Per Student/Class')
    plt.show()

    # three treeplots of reason.
    # TODO needs colours rationalized to match

    e = edg.groupby(['school_year', 'reason']).count()['index'].reset_index()
    e.sort_values(['school_year', 'index'], ascending=False, inplace=True)
    for sy in e.school_year.unique():
        squarify.plot(sizes=e.loc[e['school_year'] == sy, 'index'],
                      label=e.loc[e['school_year'] == sy, 'reason'],
                      alpha=.5)
        plt.axis('off')
        plt.show()

    # faceted barplot of changes by year/reason
    q = sns.catplot(x='reason', col='school_year', data=edg,
                    kind="count")
    q.set_xticklabels(rotation=90)
    plt.subplots_adjust(bottom=0.3, left=.05)
    plt.show()

    r = sns.catplot(x='sy_month', col='school_year', data=edg, kind='count')
    plt.show()

    gpd = edg.groupby(['school_year', 'sy_month']).count()['index'].reset_index()
    s = sns.boxplot(x='sy_month', y='index', data=gpd)
    plt.show()

def send_to_cytoscape(g, n, e):
    p4c.map_table_column()
    p4c.create_network_from_data_frames(nodes = n, edges = e, title = 'from dataframe', node_id_list = 'rand_id',
                                        source_id_list = 'from', target_id_list = 'to')

if __name__ == '__main__':
    graph = Graph('bolt://localhost:7687', password="REDACTED")
    years = ['1718', '1819', '1920']
    schoolyears = ['17-18', '18-19', '19-20']  # we use both forms for different things
    classes = ['D3', 'D4']
    modes = ['all', 'student']

    nod, edg = load_excel(years)
    load_graph(graph, nod, edg, wipe_graph=True)
    load_catalogue(graph, schoolyears, classes, modes)

    subgraph_df = get_subgraph_stats(graph)

    algo_table = ['pageRank', 'betweenness', 'alpha.degree', 'louvain', 'labelPropagation']
    run_algos(graph, subgraph_df, algo_table)

    # Pull back graph data and send to CSV for ease of R parsing

    students = graph.run("match (s:Student) return properties(s) as p").to_data_frame()
    students = pd.json_normalize(students.p)
    students = students.drop(columns=['uniq'])
    stu_long = pd.melt(students, id_vars=['rand_id', 'gradclass', 'itdp'])
    stu_long[['algorithm', 'sy_start', 'sy_end', 'class', 'type']] = stu_long.variable.str.split('-', expand=True)
    stu_long['school_year'] = stu_long['sy_start'] + "-" + stu_long['sy_end']

    stu_long.drop(columns=['variable', 'sy_start', 'sy_end'], inplace=True)
    stu_long.dropna(inplace=True)
    algo_output = stu_long.pivot(index = ['rand_id', 'class', 'school_year', 'type'], columns='algorithm', values='value').reset_index().drop(columns = 'rand_id')

    changes = graph.run(
        """match (s:Student)-[r]-(t) 
           return s.rand_id as from, t.rand_id as to, labels(t) as target_node, r.reason as reason, r.rotation as rotation, 
           r.change as change, r.school_year as school_year, r.approved as approved, s.gradclass as gradclass""").to_data_frame()
    changes = changes.applymap(lambda x: x[0] if isinstance(x, list) else x)
    changes['student_class'] = changes['school_year'].str[3:5].astype(int) - changes['gradclass'].astype(int) + 2004

    degree = students.filter(like='alpha.degree').melt()
    degree[['algorithm', 'yearstart', 'yearend', 'class', 'include']] = degree.variable.str.split(pat='-', expand=True)
    degree['school_year'] = degree['yearstart'] + "-" + degree['yearend']

    algo_output.to_csv('algo_output.csv')
    stu_long.to_csv('stu_long.csv')
    changes.to_csv('changes.csv')
    subgraph_df.to_csv('subgraph_stats.csv')

    # plot_plots(graph, edg, degree)

    # so here we're making a specific set of networks to push to cytoscape
    # I rebuilt the cytoscape networks so often that I'm just sending it this way programmatically instead
    # of doing it by hand over and over again every time I muck something up.

    # I've lost interest in visualizing the CBCE data so I'm just dropping the noswap nodes and edges

    cyto_nodes = stu_long.loc[stu_long['type'] == 'student']
    cyto_nodes = cyto_nodes.drop(columns=['type', 'school_year']).drop_duplicates(subset = ['rand_id', 'gradclass', 'itdp', 'algorithm', 'class'])
    cyto_nodes = cyto_nodes.pivot(index = ['rand_id', 'gradclass', 'itdp'], columns= ['class','algorithm'], values='value')
    cyto_nodes.columns = ['_'.join(col) for col in cyto_nodes.columns.values]
    cyto_nodes.reset_index(inplace=True)
    cyto_nodes.rename(columns={'rand_id':'id'}, inplace=True)
    cyto_nodes['name'] = cyto_nodes['id']

    cyto_edges = changes.loc[changes['target_node'] == 'Student']
    cyto_edges = cyto_edges.rename(columns={'from':'source', 'to':'target'})
    cyto_edges = cyto_edges[cyto_edges['source']!= '34d3ab87-181d-43d0-b5d9-790d3aaf6e5a']

    # go ahead and load the network directly from neo4j - it's easier that way
    # match(n:Student)
    # optional match (n)-[r]->(m:Student) return n,r, m

    # Now let's add our nicer algo-vals to the table.
    p4c.load_table_data(cyto_nodes, data_key_column='id', table_key_column='rand_id')
    # p4c.load_table_data(cyto_)
    # p4c.apply_filter('Class of 2020', hide=True)
    # p4c.apply_filter('Edges 19-20')

    p4c.create_composite_filter('19-20 D3', ['Class of 2020', 'Edges 19-20'], type='ALL')
