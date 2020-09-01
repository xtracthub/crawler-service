
from .base import Crawler
import os
from boxsdk import OAuth2, Client


class BoxCrawler(Crawler):
    def __init__(self, crawl_id, creds=None, grouper_name="lines_in_file"):
        Crawler.__init__(self)
        self.crawl_id = crawl_id
        self.creds = creds
        self.grouper_name = grouper_name

    def crawl(self):
        oauth = OAuth2(
          client_id=os.environ["box_client_id"],
          client_secret=os.environ["box_client_secret"],
        )
        client = Client(oauth)
        user = client.user().get()
        print("The current user is {0}".format(user.id))

        # root_folder = client.folder(folder_id='0')
        # shared_folder = root_folder.create_subfolder('shared_folder')
        # uploaded_file = shared_folder.upload('/path/to/file')
        # shared_link = shared_folder.get_shared_link()
        root_folder = client.folder(folder_id='112657269903').get()

        print('Folder "{0}" has {1} items in it'.format(
            root_folder.name,
            root_folder.item_collection['total_count'],
        ))


        covid_folder = client.folder(folder_id='112655002075').get()
        print('COVID folder "{0}" has {1} items in it'.format(
            covid_folder.name,
            covid_folder.item_collection['total_count'],
        ))

        print(dir(covid_folder))

        return covid_folder

