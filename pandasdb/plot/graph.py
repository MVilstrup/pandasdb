import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
import copy

def neighbour_colors(G):
    coloring = []
    cmap = sns.color_palette("RdYlGn", 3).as_hex()[::-1]

    for node in G.nodes:
        num_neigh = len([n for n in nx.all_neighbors(G, node)])

        if num_neigh <= 1:
            color = cmap[0]
        elif num_neigh == 2:
            color = cmap[1]
        else:
            color = cmap[2]
        coloring.append(color)

    return coloring


def draw_graph(G, figsize=(24, 12)):
    # There are graph layouts like shell, spring, spectral and random.
    # Shell layout usually looks better, so we're choosing it.
    # I will show some examples later of other layouts
    fig, ax = plt.subplots(1, 1, figsize=figsize)

    graph_pos = None
    try:
        graph_pos = nx.nx.nx_pydot.pydot_layout(G)
    except:
        graph_pos = nx.planar_layout(G)

    margin = max(figsize[0] / 2, 24)

    # draw nodes, edges and labels
    nx.draw_networkx_nodes(G, graph_pos, node_size=1000, node_color=neighbour_colors(G), alpha=0.7)
    nx.draw_networkx_edges(G, graph_pos, arrowsize=22, min_target_margin=margin, min_source_margin=margin)
    nx.draw_networkx_labels(G, graph_pos, font_size=12, font_family='sans-serif')

    # show graph
    plt.show()
