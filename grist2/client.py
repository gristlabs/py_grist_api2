import json
import logging
import os.path
import sys

from requests import Session
from requests.exceptions import ConnectionError

from grist2.exceptions import APIError
from grist2.utils import retry, join_urls, strip_prefix, UNSET, passed_kwargs

# Set environment variable GRIST_LOGLEVEL=DEBUG for more verbosity, WARNING for less.
log = logging.getLogger("grist_api")


def init_logging():
    if not log.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(name)s %(message)s'))
        log.setLevel(os.environ.get("GRIST_LOGLEVEL", "INFO"))
        log.addHandler(handler)
        log.propagate = False


def get_api_key():
    key = os.environ.get("GRIST_API_KEY")
    if key:
        return key
    key_path = os.path.expanduser("~/.grist-api-key")
    if os.path.exists(key_path):
        with open(key_path, "r") as key_file:
            return key_file.read().strip()
    raise KeyError("Grist API key not found in GRIST_API_KEY env, nor in %s" % key_path)


class Client:
    def __init__(
            self,
            server='https://docs.getgrist.com/',
            base_url='/api/',
            api_key=None,
            session=None,
            dryrun=False,
    ):
        """
        Initialize a client with the API Key (available from user settings),
        and optionally a server URL. If dryrun is true, will not make any changes to
        the doc. The API key, if omitted, is taken from GRIST_API_KEY env var, or ~/.grist-api-key file.
        """
        self.dryrun = dryrun
        self.server = server
        self.base_url = strip_prefix(base_url, server)
        assert not (api_key and session), "A Client can't take both api_key and an existing session"

        if session:
            self.session = session
        else:
            self.session = Session()
            api_key = api_key or get_api_key()

            self.session.headers.update({
                'Authorization': 'Bearer %s' % api_key,
            })

    @retry(5, ConnectionError, log)
    def request(self, method, url='', **kwargs):
        url = join_urls(self.full_url, url)

        if self.dryrun and method != 'GET':
            log.info("DRYRUN NOT sending %s request to %s", method, url)
            return None

        log.debug("sending %s request to %s", method, url)
        response = self.session.request(method, url, **kwargs)

        if response.status_code in (502, 503, 504):
            raise ConnectionError('Server returned code %s' % response.status_code)

        try:
            result = response.json()
        except ValueError:
            raise APIError(url, response, message='Failed to parse JSON')

        error = None
        if isinstance(result, dict):
            error = result.get('error')

        if 'SQLITE_BUSY' in str(error):
            raise ConnectionError('Grist server returned SQLITE_BUSY')

        if error or not response.ok:
            raise APIError(url, response, response_json=result)

        return result

    def get(self, url='', **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.request('GET', url, **kwargs)

    def options(self, url='', **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url='', **kwargs):
        kwargs.setdefault('allow_redirects', False)
        return self.request('HEAD', url, **kwargs)

    def post(self, url='', **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url='', **kwargs):
        return self.request('PUT', url, **kwargs)

    def patch(self, url='', **kwargs):
        return self.request('PATCH', url, **kwargs)

    def delete(self, url='', **kwargs):
        log.info('Sending DELETE request to %s', join_urls(self.base_url, url))
        return self.request('DELETE', url, **kwargs)

    def at(self, base_url=None):
        return type(self)(
            server=self.server,
            base_url=base_url or self.base_url,
            session=self.session,
            dryrun=self.dryrun,
        )

    @property
    def full_url(self):
        return join_urls(self.server, self.base_url)

    def __truediv__(self, other):
        """
        Returns a copy of this client with `other` appended to the base URL.

        For example, given `c = Client()`:

        (c / 'api/docs' / 3) will have the base URL '/api/docs/3'
        """
        return self.at(base_url=join_urls(self.base_url, other))

    @property
    def parent(self):
        """
        Returns a copy of this client with the last part of the base URL removed.
        """
        return self.at(base_url=self.base_url.rstrip('/').rsplit('/', 1)[0])

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class APIBase:
    def __init__(self, client):
        self.client = client

    def __str__(self):
        return self.__class__.__name__ + '@' + self.client.full_url

    @classmethod
    def at(cls, client, base_url=None):
        return cls(client.at(base_url))

    def __truediv__(self, other):
        return self.client / other

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class GristAPI(APIBase):
    def __init__(
            self,
            server='https://docs.getgrist.com/',
            base_url='/api/',
            api_key=None,
            dryrun=False,
    ):
        super().__init__(Client(server, base_url, api_key, dryrun=dryrun))

    @property
    def Orgs(self):
        return Orgs.at(self / 'orgs')

    def Org(self, org_id):
        return self.Orgs.Org(org_id)

    def Workspace(self, workspace_id):
        return Workspace.at(self / 'workspaces' / workspace_id)

    def Doc(self, doc_id):
        return Doc.at(self / 'docs' / doc_id)


class WithAccess(APIBase):
    def list_users(self):
        return self.client.get('access')

    def update_user_access(self, users=UNSET, maxInheritedRole=UNSET):
        delta = passed_kwargs(users=users, maxInheritedRole=maxInheritedRole)
        return self.client.patch('access', json={'delta': delta})


class WithListRecords(APIBase):
    def list(self, filters=UNSET, sort_by=UNSET, limit=UNSET):
        params = passed_kwargs(filters=filters, sort_by=sort_by, limit=limit)
        if filters is not UNSET:
            params['filters'] = json.dumps(filters)
        # TODO extract records, id, fields
        #   parse cell values
        #   handle errors
        return self.client.get(params=params)


class Orgs(APIBase):
    """
    # https://support.getgrist.com/api/#tag/orgs
    >>> orgs = test_api.Orgs

    # https://support.getgrist.com/api/#tag/orgs/paths/~1orgs/get
    >>> orgs.list()
    GET https://docs.getgrist.com/api/orgs
    """

    def Org(self, org_id):
        return Org.at(self / org_id)

    def list(self):
        return self.client.get()


class Org(WithAccess):
    """
    # https://support.getgrist.com/api/#tag/orgs
    >>> org = test_api.Org(123)

    # https://support.getgrist.com/api/#tag%2Forgs%2Fpaths%2F~1orgs~1%7BorgId%7D%2Fget
    >>> org.describe()
    GET https://docs.getgrist.com/api/orgs/123

    # https://support.getgrist.com/api/#tag%2Forgs%2Fpaths%2F~1orgs~1%7BorgId%7D%2Fpatch
    >>> org.modify(name='New Name')
    PATCH https://docs.getgrist.com/api/orgs/123
        json={'name': 'New Name'}

    >>> org.delete()
    DELETE https://docs.getgrist.com/api/orgs/123

    # https://support.getgrist.com/api/#tag%2Forgs%2Fpaths%2F~1orgs~1%7BorgId%7D~1access%2Fget
    >>> org.list_users()
    GET https://docs.getgrist.com/api/orgs/123/access

    # https://support.getgrist.com/api/#tag%2Forgs%2Fpaths%2F~1orgs~1%7BorgId%7D~1access%2Fpatch
    >>> org.update_user_access(users={'user1': 'owners'})
    PATCH https://docs.getgrist.com/api/orgs/123/access
        json={'delta': {'users': {'user1': 'owners'}}}
    """
    # TODO delete is undocumented

    @property
    def Workspaces(self):
        return Workspaces.at(self / 'workspaces')

    def describe(self):
        return self.client.get()

    def modify(self, *, name):
        return self.client.patch(json={'name': name})

    def update_user_access(self, *, users):  # noqa no maxInheritedRole for orgs
        return super().update_user_access(users=users)

    def delete(self):
        return self.client.delete()


class Workspaces(APIBase):
    """
    # https://support.getgrist.com/api/#tag/workspaces
    >>> workspaces = test_api.Org(123).Workspaces

    # https://support.getgrist.com/api/#tag%2Forgs%2Fpaths%2F~1orgs~1%7BorgId%7D~1workspaces%2Fget
    >>> workspaces.list()
    GET https://docs.getgrist.com/api/orgs/123/workspaces

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1orgs~1%7BorgId%7D~1workspaces%2Fpost
    >>> workspaces.create('Test Workspace')
    POST https://docs.getgrist.com/api/orgs/123/workspaces
        json={'name': 'Test Workspace'}
    """
    def list(self):
        return self.client.get()

    def create(self, name):
        return self.client.post(json={'name': name})


class Workspace(WithAccess):
    """
    # https://support.getgrist.com/api/#tag/workspaces
    >>> workspace = test_api.Workspace(456)

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D%2Fget
    >>> workspace.describe()
    GET https://docs.getgrist.com/api/workspaces/456

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D%2Fpatch
    >>> workspace.modify(name='New Name')
    PATCH https://docs.getgrist.com/api/workspaces/456
        json={'name': 'New Name'}

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D%2Fdelete
    >>> workspace.delete()
    DELETE https://docs.getgrist.com/api/workspaces/456

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D~1access%2Fget
    >>> workspace.list_users()
    GET https://docs.getgrist.com/api/workspaces/456/access

    # https://support.getgrist.com/api/#tag%2Fworkspaces%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D~1access%2Fpatch
    >>> workspace.update_user_access(users={'user1': 'owners'})
    PATCH https://docs.getgrist.com/api/workspaces/456/access
        json={'delta': {'users': {'user1': 'owners'}}}
    """
    @property
    def Docs(self):
        return Docs.at(self / 'docs')

    def describe(self):
        return self.client.get()

    def modify(self, *, name):
        return self.client.patch(json={'name': name})

    def delete(self):
        return self.client.delete()


class Docs(APIBase):
    """
    # https://support.getgrist.com/api/#tag/docs
    >>> docs = test_api.Workspace(456).Docs

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1workspaces~1%7BworkspaceId%7D~1docs%2Fpost
    >>> docs.create('Test Doc')
    POST https://docs.getgrist.com/api/workspaces/456/docs
        json={'name': 'Test Doc', 'isPinned': False}
    """
    def create(self, name, isPinned=False):
        return self.client.post(json={'name': name, 'isPinned': isPinned})


class Doc(WithAccess):
    """
    # https://support.getgrist.com/api/#tag/docs
    >>> doc = test_api.Doc(789)

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D%2Fget
    >>> doc.describe()
    GET https://docs.getgrist.com/api/docs/789

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D%2Fpatch
    >>> doc.modify(name='New Name')
    PATCH https://docs.getgrist.com/api/docs/789
        json={'name': 'New Name'}

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D%2Fdelete
    >>> doc.delete()
    DELETE https://docs.getgrist.com/api/docs/789

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1access%2Fget
    >>> doc.list_users()
    GET https://docs.getgrist.com/api/docs/789/access

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1access%2Fpatch
    >>> doc.update_user_access(users={'user1': 'owners'})
    PATCH https://docs.getgrist.com/api/docs/789/access
        json={'delta': {'users': {'user1': 'owners'}}}

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1move%2Fpatch
    >>> doc.move(456)
    PATCH https://docs.getgrist.com/api/docs/789/move
        json={'workspace': 456}

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1download%2Fget
    >>> doc.download()
    GET https://docs.getgrist.com/api/docs/789/download

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1download~1xlsx%2Fget
    >>> doc.download_xlsx()
    GET https://docs.getgrist.com/api/docs/789/download/xlsx

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1download~1csv%2Fget
    >>> doc.download_csv('Table1')
    GET https://docs.getgrist.com/api/docs/789/download/csv
        params={'tableId': 'Table1'}
    """
    def Table(self, table_id):
        return Table.at(self / 'tables' / table_id)

    @property
    def Attachments(self):
        return Attachments.at(self / 'attachments')

    def Attachment(self, attachment_id):
        return self.Attachments.Attachment(attachment_id)

    def describe(self):
        return self.client.get()

    def modify(self, *, name=UNSET, isPinned=UNSET):
        return self.client.patch(json=passed_kwargs(name=name, isPinned=isPinned))

    def move(self, workspace_id):
        return self.client.patch('move', json={'workspace': workspace_id})

    def delete(self):
        return self.client.delete()

    # TODO actually download to a file in all these methods.

    def download(self):
        return self.client.get('download')

    def download_xlsx(self):
        return self.client.get('download/xlsx')

    def download_csv(self, table_id):
        return self.client.get('download/csv', params={'tableId': table_id})


class Table(APIBase):
    """
    # https://support.getgrist.com/api/#tag/docs
    >>> table = test_api.Doc(789).Table('Table1')

    # https://support.getgrist.com/api/#tag%2Fcolumns%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1columns%2Fget
    >>> table.columns()
    GET https://docs.getgrist.com/api/docs/789/tables/Table1/columns

    # https://support.getgrist.com/api/#tag%2Fdocs%2Fpaths%2F~1docs~1%7BdocId%7D~1download~1csv%2Fget
    >>> table.download_csv()
    GET https://docs.getgrist.com/api/docs/789/download/csv
        params={'tableId': 'Table1'}
    """
    @property
    def Records(self):
        return Records.at(self / 'records')

    def columns(self):
        return self.client.get('columns')

    @property
    def table_id(self):
        return self.client.base_url.rstrip('/').split('/')[-1]

    def download_csv(self):
        return Doc.at(self.client.parent.parent).download_csv(self.table_id)


class Records(WithListRecords):
    """
    # https://support.getgrist.com/api/#tag/records
    >>> records = test_api.Doc(789).Table('Table1').Records

    # https://support.getgrist.com/api/#tag%2Frecords%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1records%2Fget
    >>> records.list(filters={"pet": ["cat", "dog"]}, sort_by="pet,-age", limit=5)
    GET https://docs.getgrist.com/api/docs/789/tables/Table1/records
        params={'filters': '{"pet": ["cat", "dog"]}', 'sort_by': 'pet,-age', 'limit': 5}

    # https://support.getgrist.com/api/#tag%2Frecords%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1records%2Fpost
    >>> records.create([{"pet": "cat", "age": 3}, {"pet": "dog", "age": 5}])
    POST https://docs.getgrist.com/api/docs/789/tables/Table1/records
        json={'records': [{'fields': {'pet': 'cat', 'age': 3}}, {'fields': {'pet': 'dog', 'age': 5}}]}

    # https://support.getgrist.com/api/#tag%2Frecords%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1records%2Fpatch
    >>> records.modify([{"id": 1, "age": 4}])
    PATCH https://docs.getgrist.com/api/docs/789/tables/Table1/records
        json={'records': [{'id': 1, 'fields': {'age': 4}}]}

    # https://support.getgrist.com/api/#tag%2Frecords%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1records%2Fput
    >>> records.create_or_modify()
    PUT https://docs.getgrist.com/api/docs/789/tables/Table1/records

    # https://support.getgrist.com/api/#tag%2Fdata%2Fpaths%2F~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1data~1delete%2Fpost
    >>> records.delete()
    POST https://docs.getgrist.com/api/docs/789/tables/Table1/data/delete
    """
    # TODO from the original library: chunking, sync_table

    def create(self, records, *, parse_strings=True):
        records = [{"fields": record} for record in records]
        # TODO extract records, id
        return self.client.post(json=dict(records=records), params=self._noparse(parse_strings))

    def modify(self, records, *, parse_strings=True):
        records = [{"id": record.pop("id"), "fields": record} for record in records]
        return self.client.patch(json=dict(records=records), params=self._noparse(parse_strings))

    def create_or_modify(self):
        # TODO
        return self.client.put()

    def delete(self):
        self.client.parent.post('data/delete')

    def _noparse(self, parse_strings):
        if parse_strings:
            return {}
        return {'noparse': 'true'}


class Attachments(WithListRecords):
    """
    # https://support.getgrist.com/api/#tag/attachments
    >>> attachments = test_api.Doc(789).Attachments

    # https://support.getgrist.com/api/#tag%2Fattachments%2Fpaths%2F~1docs~1%7BdocId%7D~1attachments%2Fget
    >>> attachments.list()
    GET https://docs.getgrist.com/api/docs/789/attachments

    # https://support.getgrist.com/api/#tag%2Fattachments%2Fpaths%2F~1docs~1%7BdocId%7D~1attachments%2Fpost
    >>> attachments.create()
    POST https://docs.getgrist.com/api/docs/789/attachments
    """
    def create(self):  # should this be called `upload`?
        # TODO
        return self.client.post()

    def Attachment(self, attachment_id):
        return Attachment.at(self / attachment_id)


class Attachment(APIBase):
    """
    # https://support.getgrist.com/api/#tag/attachments
    >>> attachment = test_api.Doc(789).Attachment(123)

    # https://support.getgrist.com/api/#tag%2Fattachments%2Fpaths%2F~1docs~1%7BdocId%7D~1attachments~1%7BattachmentId%7D%2Fget
    >>> attachment.describe()
    GET https://docs.getgrist.com/api/docs/789/attachments/123

    # https://support.getgrist.com/api/#tag%2Fattachments%2Fpaths%2F~1docs~1%7BdocId%7D~1attachments~1%7BattachmentId%7D~1download%2Fget
    >>> attachment.download()
    GET https://docs.getgrist.com/api/docs/789/attachments/123/download
    """

    def describe(self):
        return self.client.get()

    def download(self):
        # TODO
        return self.client.get('download')


class DocTestClient(Client):
    def request(self, method, url='', **kwargs):
        url = join_urls(self.full_url, url)
        message = f"{method} {url}"
        if kwargs:
            for key, value in kwargs.items():
                if key not in ["json", "params"] or not value:
                    continue
                message += f"\n    {key}={value}"

        print(message)


test_api = GristAPI(api_key='test')
test_api.client = DocTestClient(api_key='test')
