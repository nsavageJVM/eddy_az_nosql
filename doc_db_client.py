import sys

import pydocumentdb.documents as documents
import pydocumentdb.errors as errors
import pydocumentdb.document_client as document_client

from db_config import config as cfg

from pip._vendor.distlib.compat import raw_input

HOST = cfg.settings['host']
MASTER_KEY = cfg.settings['master_key']
DATABASE_ID = cfg.settings['database_id']
COLLECTION_ID = cfg.settings['collection_id']

database_link = 'dbs/' + DATABASE_ID

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
                    print("\nrcreating db done")

        if command == 'delete-db':
            print("delete db")

            with IDisposable(document_client.DocumentClient(HOST, {'masterKey': MASTER_KEY})) as client:
                try:
                    delete_database(client, DATABASE_ID)
                except errors.HTTPFailure as e:
                    print('\ndelete db has caught an error. {0}'.format(e.message))

                finally:
                    print("\ndelete db done")

        elif command == 'QQ':
            break
        else:
            print('Invalid Command\n')


if __name__ == "__main__":
	main(sys.argv)