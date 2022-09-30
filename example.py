from grist2 import GristAPI
from pprint import pprint

api = GristAPI()

pprint(api.Orgs.list())

org = api.Org(2)
pprint(org.describe())
pprint(org.list_users())

workspaces = org.Workspaces
# workspace_id = workspaces.create(name='Test Workspace')
pprint(workspaces.list())

workspace = api.Workspace(2)
pprint(workspace.describe())

doc_id = workspace.Docs.create(name='Test Doc')
print(doc_id)

doc = api.Doc(doc_id)
pprint(doc.describe())

table = doc.Table('Table1')
pprint(table.columns())

records = table.Records
pprint(records.create([{"A": 1, "B": 2}, {"A": 3, "B": 4}]))
pprint(records.list())

attachments = doc.Attachments
pprint(attachments.list())
