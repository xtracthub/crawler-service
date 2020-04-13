
from materials_io.utils.interface import get_parser, get_available_parsers
from uuid import uuid4
import itertools


import networkx as nx


class MatIOGrouper:

    def __init__(self):
        self.name = "matio"  # TODO: Add to parent class.
        self.soft_max_files = 50
        self.hard_max_files = 10

    def make_file_graph(self, group_dict):

        # for group in group_dict:

        num_g_1 = 0
        print("Assembling graph")
        gr = nx.Graph()
        too_long_comps = []
        groups = []
        for parser in group_dict:
            print(parser)
            for item in group_dict[parser]:
                # print(item)
                if len(item) > 1:
                    # print(item)
                    num_g_1 += 1
                    if len(item) >= self.hard_max_files:
                        print("TOO BIG")
                        print(len(item))
                        exit()
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
        print(f"Type of conn_comps: {type(conn_comps)}")

        ### DEBUG

        # for item in conn_comps:
            # print(type(item))

        ###

        # for item in too_long_comps:
        #     conn_comps.append(set(item))
        # conn_comps.append(set(too_long_comps))
        print(f"Number of connected components: {len(conn_comps)}")

        print("Now counting nodes and edges...")

        # exit()
        print(f"Number of nodes: {gr.number_of_nodes()}")
        print(f"Number of edges: {gr.number_of_edges()}")

        # So at this point, conn_comps is all files that must travel together.
        return conn_comps

    def pack_groups(self, conn_comps, file_groups_map, group_files_map, strategy='minimum'):
        """ Input dict of all MatIO groups,
            Output 'minimum' dict of necessary MatIO families (for 'transfer only once' processing).
            # TODO: Support other strategies -- like 'full directory' and set_size.
        """
        # Generate a unique identifier for each 'family' of groups (e.g., groups that must travel together).

        families = {}
        for comp in conn_comps:
            family_uuid = str(uuid4())

            for filename in comp:
                # print(f"Filename: {filename}")
                gr_ids = file_groups_map[filename]
                # print(f"Group IDs: {gr_ids}")

                families[family_uuid] = {"groups": {}}

                for gr_id in gr_ids:
                    families[family_uuid]["groups"][gr_id] = group_files_map[gr_id]

        return families

    def group(self, file_ls):
        # Run each grouping and then each parser. Emulate the behavior in
        # https://github.com/materials-data-facility/MaterialsIO/blob/master/materials_io/utils/interface.py

        # Index and inverted index of files and groups
        file_groups_map = {}
        group_files_map = {}

        parser_desc = get_available_parsers()
        parsers = [*parser_desc]  # This is a list of all applicable parsers.

        group_coll = {}

        for parser in parsers:
            print(f"Processing parser: {parser}")

            if parser in ['noop', 'generic']:
                continue

            p = get_parser(parser)
            group = p.group(file_ls)

            gr_list = list(group)

            # group_coll[parser] = gr_list
            group_coll[parser] = []

            for gr in gr_list:

                # TODO: Odd assumption to be making. Should allow groups to be arbitrarily large.
                if len(gr) > self.hard_max_files:
                    print("The group was too dang big. ")
                    print(len(gr))
                    continue

                # Assign a group_id for each group.
                group_id = str(uuid4())
                for filename in gr:
                    # print(gr_list)
                    if filename not in file_groups_map:
                        file_groups_map[filename] = [group_id]
                    file_groups_map[filename].append(group_id)

                    group_files_map[group_id] = {"files": [], "parser": parser}
                    group_files_map[group_id]["files"].append(filename)
                group_coll[parser].append(gr)

                if len(gr) > self.hard_max_files:

                    print("HOW THE FUCK DID WE GET HERE????")

        # Get the connected components of the graph.
        conn_comps = self.make_file_graph(group_coll)

        # Use the connected components to generate a family for each connected component.
        families = self.pack_groups(conn_comps, file_groups_map, group_files_map)

        print(f"Generated {len(families)} mutually exclusive families of file-groups... Terminating...")

        return families
