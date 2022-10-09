import boto3

DRY_RUN = False
VERSIONS_TO_RETAIN = 2

def nextFetch(func, args, k):
  result = []

  while True:
    response = func(**args)

    if not response[k]:
      break
    
    result += response[k]

    if 'NextToken' in response:
      args['NextToken'] = response['NextToken']
    else:
      break

  return result



# CODE

if VERSIONS_TO_RETAIN < 2:
  exit('[ERROR] Target retain must be greater than 2.')

if DRY_RUN:
  print('[INFO] Dry run.')

glue = boto3.client('glue')


databases = glue.get_databases()

if not databases['DatabaseList']:
  exit('[INFO] Databases not found.')

databases = [d['Name'] for d in databases['DatabaseList']]

for database in databases:
  print('[INFO] Processing database %s' % database)

  for t in nextFetch(glue.get_tables, { 'DatabaseName': database }, 'TableList'):
    table = t['Name']

    versionIds = [v['VersionId'] for v in nextFetch(glue.get_table_versions, {
      'DatabaseName': database,
      'TableName': table
    }, 'TableVersions')]

    numVersions = len(versionIds)

    print('[INFO] Processing table %s.%s (number of version: %s)' % (database, table, numVersions))

    if numVersions > VERSIONS_TO_RETAIN:
      versionsToDelete = versionIds[VERSIONS_TO_RETAIN:]

      while nextBatchOfVersionIdsToDelete := versionsToDelete[:100]:
        if DRY_RUN:
          print(nextBatchOfVersionsToDelete)
        else:
          glue.batch_delete_table_version(
            DatabaseName = database,
            TableName = table,
            VersionIds = nextBatchOfVersionIdsToDelete
          )

          print('[INFO] Versions deleted %s' % nextBatchOfVersionIdsToDelete)
        
        versionsToDelete = versionsToDelete[100:]
        



print(databases)