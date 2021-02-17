
import itertools
import networkx as nx
import matplotlib.pyplot as plt


groups = [[1], [1], [2], [3]]

G = nx.Graph()

for group in groups:
    # Add nodes
    for filename in group:
        G.add_node(filename)
    # Add edges
    all_edges = list(itertools.combinations(group, 2))

    for edge in all_edges:
        G.add_edge(*edge)  # '*' to unpack the tuple.


plt.subplot(121)
nx.draw(G, with_labels=True, font_weight="bold")

plt.show()