
from googleapiclient.discovery import build


def generate_drive_connection(creds):
    service = build('drive', 'v3', credentials=creds)
    return service


def get_next_page(service, nextPageToken):
    results = service.files().list(
        pageSize=1000, pageToken=nextPageToken,
        fields="nextPageToken, files(id, name, size, mimeType, fullFileExtension)").execute()
    return results

