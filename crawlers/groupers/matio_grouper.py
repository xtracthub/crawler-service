
from materials_io.utils.interface import get_parser, get_available_parsers
from uuid import uuid4
import itertools
import time


import networkx as nx


class MatIOGrouper:

    def __init__(self, logger):
        self.name = "matio"  # TODO: Add to parent class.
        self.soft_max_files = 50
        self.hard_max_files = 10
        self.logger = logger

    def make_file_graph(self, group_dict):

        # for group in group_dict:

        num_g_1 = 0
        self.logger.debug("Assembling graph")
        gr = nx.Graph()
        groups = []
        for parser in group_dict:
            for item in group_dict[parser]:
                if len(item) > 1:
                    num_g_1 += 1
                    self.logger.debug(f"Number of groups with more than 1 element: {num_g_1}")
                    self.logger.debug(f"Len of largest group: {len(item)}")
                groups.append(item)

        self.logger.debug("Generating nodes and edges... ")
        self.logger.debug(f"Number of groups: {len(groups)}")
        for group in groups:

            # Add nodes
            for filename in group:
                gr.add_node(filename)
            # Add edges
            all_edges = list(itertools.combinations(group, 2))

            for edge in all_edges:
                gr.add_edge(*edge)  # '*' to unpack the tuple.

        self.logger.debug("Generating number of connected components...")
        conn_comps = sorted(nx.connected_components(gr), key=len, reverse=True)

        self.logger.debug(f"Number of connected components: {len(conn_comps)}")
        self.logger.debug("Now counting nodes and edges...")
        self.logger.debug(f"Number of nodes: {gr.number_of_nodes()}")
        self.logger.debug(f"Number of edges: {gr.number_of_edges()}")
        gr.clear()

        # So at this point, conn_comps is all files that must travel together.
        # TODO: Why are non-connected
        return conn_comps

    def pack_groups(self, conn_comps, file_groups_map, group_files_map, strategy='minimum'):
        """ Input dict of all MatIO groups,
            Output 'minimum' dict of necessary MatIO families (for 'transfer only once' processing).
            # TODO: Support other strategies -- like 'full directory' and set_size.
        """
        # Generate a unique identifier for each 'family' of groups (e.g., groups that must travel together).

        families = []
        for comp in conn_comps:

            print(f"Len of connected component: {len(comp)}")

            family_uuid = str(uuid4())

            # Create a new family with a fresh UUID.
            family = {"family_id": family_uuid, "files": [], "groups": []}

            # Note here that every filename in the comp is associated with a single family id.
            for filename in comp:

                gr_ids = file_groups_map[filename]
                # self.logger.debug(f"All groups containing file {filename}: {gr_ids}")

                # Add to the cumulative list of unique files across all groups in a family.
                family["files"].append(filename)

                for gr_id in gr_ids:
                    # print(f"ASSOCIATED FILES: {group_files_map[gr_id]}")
                    gr_info = group_files_map[gr_id]
                    family["groups"].append({"group_id": gr_id, "parser": gr_info["parser"], "files": gr_info["files"]})
            families.append(family)
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

            if parser in ['noop', 'generic']:
                continue

            p = get_parser(parser)
            group = p.group(file_ls)

            gr_list = list(group)
            group_coll[parser] = []

            for gr in gr_list:
                if len(gr) > self.hard_max_files:
                    self.logger.warning(f"Proposed group too large ({len(gr)}). Skipping...")
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

        # Get the connected components of the graph.
        t_graph_start = time.time()
        conn_comps = self.make_file_graph(group_coll)
        t_graph_end = time.time()

        self.logger.info(f"Total time to build graph: {t_graph_end - t_graph_start}")

        # Use the connected components to generate a family for each connected component.
        t_group_pack_start = time.time()
        families = self.pack_groups(conn_comps, file_groups_map, group_files_map)
        t_group_pack_end = time.time()

        self.logger.info(f"Total time to pack groups: {t_group_pack_end - t_graph_start}")
        self.logger.debug(f"Generated {len(families)} mutually exclusive families of file-groups... Terminating...")

        print(f"Length of families: {len(families)}")
        print(f"One family: {families[0]}")
        return families
