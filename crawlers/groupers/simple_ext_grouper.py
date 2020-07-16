
from xtract_sdk.packagers.family import Family


class SimpleExtensionGrouper:
    def __init__(self, creds, by_file=True, logger=None):
        self.logger = logger
        self.by_file = True
        self.max_bytes = 1073741824  # 1 GB

        # TODO: Eventually we want to add creds here, but for now we re-inject at Orchestration.
        self.creds = creds

    # TODO: Create a 'mappings' file that just contains a bunch of these sets.
    def get_mappings(self):
        text_types = {'txt', 'doc', 'docx', 'rtf', 'dotx', 'dot', 'odt',
                          'pages', 'tex', 'pdf', 'ps', 'eps', 'prn'}

        tabular_types = {'tsv', 'csv', 'xls', 'xlsx', 'xltx', 'xlt', 'ods',
                             'xlsb', 'xlsm', 'xltm'}

        image_types = {'jpg', 'jpeg', 'png', 'tiff', 'tif', 'gif', 'bmp'}

        return {"text": text_types, "tabular": tabular_types, "images": image_types}

    def gen_families(self, file_ls):
        """Given list of metadata dicts, output updated list of extractors

            NOTE FOR THIS GROUPER :: 1 file = 1 family = 1 group = 1 file """
        if not self.by_file:
            raise ValueError("Unable to process groups of more than 1 file by extension!")

        families = []

        mappings = self.get_mappings()

        for fdict in file_ls:

            groups = []
            valid_mapping = False
            mimeType = None
            for mapping in mappings:

                if fdict['extension'].lower() in mappings[mapping]:
                    # TODO: this will eventually need to be a list of extractors.
                    fdict['extractor'] = mapping  # mapping = extractor_name!
                    valid_mapping = True
                    mimeType = fdict["mimeType"]

            if not valid_mapping:
                mimeType = fdict["mimeType"]
                if 'vnd.google-apps.document' in mimeType:
                    fdict['extractor'] = "text"
                    mimeType = "text/plain"
                elif 'vnd.google-apps.spreadsheet' in mimeType:
                    fdict['extractor'] = "tabular"
                    mimeType = "text/csv"
                elif 'vnd.google-apps.presentation' in mimeType:
                    # fdict['extractor'] = "text"  # TODO: this should come back soon.
                    fdict['extractor'] = None
                    mimeType = None
                    # TODO from Will: " slides: text, tabular, images, BERT... order is not important"
                else:
                    # Now we default to None
                    fdict['extractor'] = None
                    mimeType = None

            groups.append(fdict)

            # Here we will use the Xtract family object
            family = Family(download_type="gdrive")

            family.add_group(files=[{"path": fdict["id"],
                                     "metadata": fdict,
                                     "is_gdoc": fdict["is_gdoc"],
                                     "mimeType": mimeType}],
                             parser=fdict["extractor"])

            families.append(family.to_dict())

        return families
