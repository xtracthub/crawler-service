#!/usr/bin/env python

import json
import sys
import time
import webbrowser
from queue import Queue

# from utils import enable_requests_logging, is_remote_session

from globus_sdk import (NativeAppAuthClient, TransferClient,
                        RefreshTokenAuthorizer)
from globus_sdk.exc import GlobusAPIError, TransferAPIError


CLIENT_ID = '079bdf4e-9666-4816-ac01-7eab9dc82b93'
TOKEN_FILE = 'refresh-tokens.json'
REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
SCOPES = ('openid email profile '
          'urn:globus:auth:scope:transfer.api.globus.org:all')

#TUTORIAL_ENDPOINT_ID = 'ddb59aef-6d04-11e5-ba46-22000b92c6ec'
# PETREL_ID  = '45a53408-c797-11e6-9c33-22000a1e3b52'
PETREL_ID = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'

get_input = getattr(__builtins__, 'raw_input', input)

# uncomment the next line to enable debug logging for network requests
# enable_requests_logging()


COUNT = 0


def increment():
    global COUNT
    COUNT = COUNT+1


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, 'r') as f:
        tokens = json.load(f)

    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, 'w') as f:
        json.dump(tokens, f)


def update_tokens_file_on_refresh(token_response):
    """
    Callback function passed into the RefreshTokenAuthorizer.
    Will be invoked any time a new access token is fetched.
    """
    save_tokens_to_file(TOKEN_FILE, token_response.by_resource_server)


def do_native_app_authentication(client_id, redirect_uri,
                                 requested_scopes=None):
    """
    Does a Native App authentication flow and returns a
    dict of tokens keyed by service name.
    """
    client = NativeAppAuthClient(client_id=client_id)
    # pass refresh_tokens=True to request refresh tokens
    client.oauth2_start_flow(requested_scopes=requested_scopes,
                             redirect_uri=redirect_uri,
                             refresh_tokens=True)

    url = client.oauth2_get_authorize_url()

    print('Native App Authorization URL: \n{}'.format(url))

    # if not is_remote_session():
    webbrowser.open(url, new=1)

    auth_code = get_input('Enter the auth code: ').strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # return a set of tokens, organized by resource server name
    return token_response.by_resource_server


def get_extension(filepath):
    """Returns the extension of a filepath.
    Parameter:
    filepath (str): Filepath to get extension of.
    Return:
    extension (str): Extension of filepath.
    """
    filename = filepath.split('/')[-1]
    extension = None

    if '.' in filename:
        extension = filename.split('.')[-1]

    return extension


def crawl(transfer, start_dir):

    mdata_blob = {}
    failed_dirs = {"failed": []}

    to_crawl = Queue()
    to_crawl.put(start_dir)

    while not to_crawl.empty():

        cur_dir = to_crawl.get()

        try:
            for entry in transfer.operation_ls(PETREL_ID, path=cur_dir):

                full_path = cur_dir + entry['name']
                # print(full_path)
                if entry['type'] == 'file':
                    extension = get_extension(entry["name"])
                    mdata_blob[full_path] = {"physical": {'size': entry['size'],
                                                          "extension": extension, "path_type": "globus"}}
                    increment()
                    if COUNT % 20000 == 0:
                        print("COUNT: {}".format(COUNT))

                elif entry['type'] == 'dir':
                    full_path = cur_dir + "/" + entry['name']
                    to_crawl.put(full_path)

        except TransferAPIError as e:
            print("Problem directory {}".format(cur_dir))
            print("Transfer client received the following error:")
            print(e)
            failed_dirs["failed"].append(cur_dir)
            continue

    # print(mdata_blob)
    print("FILES PROCESSED: {}".format(COUNT))

    with open('result.json', 'w') as fp:
        json.dump(mdata_blob, fp)

    with open('failed.json', 'w') as fp:
        json.dump(mdata_blob, fp)

    return mdata_blob


def main():
    tokens = None
    try:
        # if we already have tokens, load and use them
        tokens = load_tokens_from_file(TOKEN_FILE)
    except:
        pass

    if not tokens:
        # if we need to get tokens, start the Native App authentication process
        tokens = do_native_app_authentication(CLIENT_ID, REDIRECT_URI, SCOPES)

        try:
            save_tokens_to_file(TOKEN_FILE, tokens)
        except:
            pass

    transfer_tokens = tokens['transfer.api.globus.org']

    auth_client = NativeAppAuthClient(client_id=CLIENT_ID)

    authorizer = RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=update_tokens_file_on_refresh)

    transfer = TransferClient(authorizer=authorizer)

    # print out a directory listing from an endpoint
    try:
        transfer.endpoint_autoactivate(PETREL_ID)
    except GlobusAPIError as ex:
        print(ex)
        if ex.http_status == 401:
            sys.exit('Refresh token has expired. '
                     'Please delete refresh-tokens.json and try again.')
        else:
            raise ex

    # TODO: Unhardcode the crawlable directory name.
    # crawl(transfer, '/~/cdiac/cdiac.ornl.gov/pub8old/oceans')
    # crawl(transfer, '/~/MDF/mdf_connect/prod/data/_test_person_simple_v1.1/')
    
    import time 
    t0 = time.time()
    crawl(transfer, '/~/MDF/mdf_connect/prod/')
    t1 = time.time()

if __name__ == '__main__':
    main()
