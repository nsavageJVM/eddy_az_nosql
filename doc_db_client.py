import sys

import pydocumentdb.errors as errors
import pydocumentdb.document_client as document_client

from db_config import config as cfg

from pip._vendor.distlib.compat import raw_input

HOST = cfg.settings['host']
MASTER_KEY = cfg.settings['master_key']
DATABASE_ID = cfg.settings['database_id']
COLLECTION_ID = cfg.settings['collection_id']

database_link = 'dbs/' + DATABASE_ID

col = {}

#region utility class handle db connections correctly
class IDisposable:
    """ A context manager to automatically close an object with a close method
    in a with statement. """

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self = None
# endregion

#region create db
def create_database(client, id):
    print("\n2. Create Database")

    try:
        client.CreateDatabase({"id": id})
        print('Database with id \'{0}\' created'.format(id))

    except errors.DocumentDBError as e:
        if e.status_code == 409:
            print('A database with id \'{0}\' already exists'.format(id))
        else:
            raise errors.HTTPFailure(e.status_code)
#endregion

#region delete db
def delete_database(client, id):
    print("\n5. Delete Database")

    try:
        database_link = 'dbs/' + id
        client.DeleteDatabase(database_link)

        print('Database with id \'{0}\' was deleted'.format(id))

    except errors.DocumentDBError as e:
        if e.status_code == 404:
            print('A database with id \'{0}\' does not exist'.format(id))
        else:
            raise errors.HTTPFailure(e.status_code)
#endregion

#region create collection
def create_collection(client, id):
    """ Execute the most basic Create of collection.
    This will create a collection with OfferType = S1 and default indexing policy """
    global col
    print("\n2.1 Create Collection - Basic")

    try:
        col  = client.CreateCollection(database_link, {"id": id})
        print('Collection with id \'{0}\' created'.format(id))

    except errors.DocumentDBError as e:
        if e.status_code == 409:
            print('A collection with id \'{0}\' already exists'.format(id))
        else:
            raise errors.HTTPFailure(e.status_code)

#endregion

#region create collection
def create_document(client, id):
    """ Execute the most basic Create of collection.
    This will create a collection with OfferType = S1 and default indexing policy """

    print("\n2.1 Create Document")
    # https://azure.microsoft.com/en-us/documentation/articles/documentdb-python-application/
    try:
        client.CreateDocument(col['_self'],
                {'id': 'eddy-doc','filed_key': 'field_value', 'name': 'eddy-doc'  })

    except errors.DocumentDBError as e:
        if e.status_code == 409:
            print('A collection with id \'{0}\' already exists'.format(id))
        else:
            raise errors.HTTPFailure(e.status_code)

#endregion

def main(argv):
    while True:
        command = raw_input('command? QQ to quit\n ').strip()

        if command == 'create-db':
            print("creating db")

            with IDisposable(document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})) as client:
                try:
                    create_database(client, DATABASE_ID)
                except errors.HTTPFailure as e:
                    print('\ncreating db has caught an error. {0}'.format(e.message))

                finally:
                    print("\ncreating db done")

        if command == 'delete-db':
            print("delete db")

            with IDisposable(document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})) as client:
                try:
                    delete_database(client, DATABASE_ID)
                except errors.HTTPFailure as e:
                    print('\ndelete db has caught an error. {0}'.format(e.message))

                finally:
                    print("\ndelete db done")

        if command == 'create-col':
            print("create-col")

            with IDisposable(document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})) as client:
                try:
                    create_collection(client, COLLECTION_ID)
                except errors.HTTPFailure as e:
                    print('\ndelete db has caught an error. {0}'.format(e.message))

                finally:
                    print("\ndelete db done")

        if command == 'create-doc':
            print("create-document")

            with IDisposable(document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})) as client:
                try:
                    create_document(client, COLLECTION_ID)
                except errors.HTTPFailure as e:
                    print('\ncreate-document has caught an error. {0}'.format(e.message))

                finally:
                    print("\ncreate-document  done")

        elif command == 'QQ':
            break



if __name__ == "__main__":
	main(sys.argv)