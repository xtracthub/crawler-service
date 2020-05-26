
import pickle as pkl
from pydrive.drive import GoogleDrive


def g_crawl(auth_pkl):

  try:
    # auth_pkl = pkl.dumps(gauth)

    print(f"GAUTH: {auth_pkl}")

    gauth = pkl.loads(auth_pkl)

    drive = GoogleDrive(gauth)  # Create GoogleDrive instance with authenticated GoogleAuth instance

    # Auto-iterate through all files in the root folder.
    # file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
    file_list = drive.ListFile({'q': "mimeType='image/jpeg' and trashed=false"}).GetList()
    folder_list = drive.ListFile({'q': "mimeType = 'application/vnd.google-apps.folder'"}).GetList()

    for folder in folder_list:
      print('title: %s, id: %s' % (folder['title'], folder['id']))

    # for file1 in file_list:#   print('title: %s, id: %s' % (file1['title'], file1['id']))
  except Exception as e:
    return e

  return folder_list