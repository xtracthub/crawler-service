
from materials_io.utils.interface import get_parser, get_available_parsers
from uuid import uuid4
import itertools
import matplotlib.pyplot as plt


import networkx as nx


class MatIOGrouper:

    def __init__(self):
        self.name = "matio"  # TODO: Add to parent class.
        self.soft_max_files = 50
        self.hard_max_files = 10000

    def make_file_graph(self, group_dict):

        num_g_1 = 0
        print("Assembling graph")
        gr = nx.Graph()
        groups = []
        for parser in group_dict:
            print(parser)
            for item in group_dict[parser]:
                print(item)
                if len(item) > 1:
                    # print(item)
                    num_g_1 += 1
                    print(f"Number of groups with more than 1 element: {num_g_1}")
                    print(f"Len of big group: {len(item)}")
                groups.append(item)

        print("Generating nodes and edges... ")
        print(f"Number of groups: {len(groups)}")
        for group in groups:
            # Add nodes
            for filename in group:
                gr.add_node(filename)
            # Add edges
            all_edges = list(itertools.combinations(group, 2))

            for edge in all_edges:
                gr.add_edge(*edge)  # '*' to unpack the tuple.

        print("Generating number of connected components...")
        conn_comps = sorted(nx.connected_components(gr), key=len, reverse=True)
        print(len(conn_comps))

        print(f"Number of nodes: {gr.number_of_nodes()}")
        print(f"Number of edges: {gr.number_of_edges()}")
        # plt.subplot(121)
        # print("Drawing plot!")
        # nx.draw(gr, with_labels=False, font_weight="bold")
        #
        # plt.show()


    def pack_groups(self, strategy='minimum'):
        """ Input dict of all MatIO groups,
            Output 'minimum' dict of necessary MatIO families (for 'transfer only once' processing).
            # TODO: Support other strategies -- like 'full directory' and set_size.
        """

        family_uuid = uuid4()

    def group(self, file_ls):
        # Run each grouping and then each parser. Emulate the behavior in
        # https://github.com/materials-data-facility/MaterialsIO/blob/master/materials_io/utils/interface.py

        parser_desc = get_available_parsers()
        parsers = [*parser_desc]  # This is a list of all applicable parsers.

        group_coll = {}

        for parser in parsers:
            print(f"Processing parser: {parser}")

            if parser in ['noop', 'generic']:
                continue

            p = get_parser(parser)
            group = p.group(file_ls)

            group_coll[parser] = group

        self.make_file_graph(group_coll)

        return group_coll
