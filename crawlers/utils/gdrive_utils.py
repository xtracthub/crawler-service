
from googleapiclient.discovery import build


def generate_drive_connection(creds):
    service = build('drive', 'v3', credentials=creds)
    return service


def get_next_page(service, nextPageToken):
    results = service.files().list(
        # pageSize auto-reduces to 100 if 'permissions' in query-string.
        pageSize=1000, pageToken=nextPageToken,
        # TODO: add the rest of the config now.
        fields="nextPageToken, files(id, name, mimeType, fullFileExtension, size, parents, modifiedTime, shared, webViewLink, permissions)").execute()
    return results
