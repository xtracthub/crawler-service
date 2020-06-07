

class SimpleExtensionGrouper:
    def __init__(self, by_file=True, logger=None):
        self.logger = logger
        self.by_file = True
        self.max_bytes = 1073741824  # 1 GB

    # TODO: Create a 'mappings' file that just contains a bunch of these sets.
    def get_mappings(self):
        text_types = {'txt', 'doc', 'docx', 'rtf', 'dotx', 'dot', 'odt',
                          'pages', 'tex', 'pdf', 'ps', 'eps', 'prn'}

        tabular_types = {'tsv', 'csv', 'xls', 'xlsx', 'xltx', 'xlt', 'ods',
                             'xlsb', 'xlsm', 'xltm'}

        image_types = {'jpg', 'jpeg', 'png', 'tiff', 'tif', 'gif', 'bmp'}

        return {"text": text_types, "tabular": tabular_types, "images": image_types}

    def gen_groups(self, file_ls):
        """Given list of metadata dicts, output updated list of extractors """
        if not self.by_file:
            raise ValueError("Unable to process groups of more than 1 file by extension!")

        new_ls = []

        mappings = self.get_mappings()

        for fdict in file_ls:
            valid_mapping = False
            for mapping in mappings:
                if fdict['extension'] in mappings[mapping]:
                    # TODO: this will eventually need to be a list of extractors.
                    fdict['extractor'] = mapping  # mapping = extractor_name!
                    valid_mapping = True

            if not valid_mapping:
                mimetype = fdict["mimeType"]
                if 'vnd.google-apps.document' in mimetype:
                    fdict['extractor'] = "text"
                elif 'vnd.google-apps.spreadsheet' in mimetype:
                    fdict['extractor'] = "tabular"
                elif 'vnd.google-apps.presentation' in mimetype:
                    fdict['extractor'] = "text"
                    # TODO from Will: " slides: text, tabular, images, BERT... order is not important"
                else:
                    # Now we default to None
                    fdict['extractor'] = None

            new_ls.append(fdict)

        return new_ls
