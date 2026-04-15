Some time ago we added a support for dev branches (storage objects) and implemented following defference mechanism:
```
In short: List buckets / List Tables will provide only prod versions of objects, even for those that do not have a prod version yet. Only fully qualified names for query_data are built to always reference the branched version if present.


Scenario 1 (branched table created from existing production table):
There is a bucket in.c-mybucket that contains tables A and B. Branched version of B is created -> bucket in.c-123-mybucket is returned via API and contain only table B
Required behaviour:

list_buckets tool returns only in.c-mybucket (prod version) with tables A and B.

When building fully qualified name table B must reference in.c-123-mybucket.B (only for query tool)

Otherwise we are working only with production paths in IM/OMs

Scenario 2 (New table created in a branch and existing bucket):
If new table is created, it will appear only in the branched bucket.

There is a bucket in.c-mybucket that contains tables A and B. New table C is created in that bucket -> bucket in.c-123-mybucket is returned via API and contain only table C

Required behaviour:

list_buckets returns only in.c-mybucket (prod version) with all tables A,B and C

Table C will look like it exists in production already (e.g. we modify the tool response to fake it)

When building fully qualified name table C must reference in.c-123-mybucket.C (only for query tool)

Otherwise we are working only with production paths in IM/OMs

Scenario 3 (New bucket with a table is created in a branch)
If a new bucket with a table is created, the new bucket with that table will exist only in branched version (from API perspective).

New bucket in.c-newbucket with table D is created -> bucket in.c-123-newbucket is returned via API and contains table D (there is no in.c-newbucket bucket returned via API)

Required behaviour:

list_buckets returns only in.c-newbucket (prod version) with table D

Table D (and the bucket) will look like it exists in production already (e.g. we modify the tool response to fake it)

When building fully qualified name table D must reference in.c-123-newbucket.D (only for query tool)

Otherwise we are working only with production paths in IM/OMs (e.g. in.c-newbucket.D )
```

Now we added a new feature that changes how the branches work. The main changes are:
- Default branch storage api calls do not return branched buckets/tables at all, only production ones. 
- Branched tables (the ones that were created or touched in a branch) are only visible in the branch they were created in, and they are not visible in the default branch at all. e.g. the storage api calls need to call the branched endpoint
  - Note that the branched endpoint contains only the branched tables, so no production tables that were not modified are visible there.
-  The FQN of branched tables is different. Before the schema was `"DB_PATH"."out.c-BRANCH_ID-bucketname"` now when the branched storage feature is enabled it is `"DB_PATH"."BRANCH_ID_out.c-bucketname"

The UI deals with it using the following logic defined in the @UI_LEVEL_DEFERENCE_ANALYSIS.md file.

The project feature name is `storage-branches`

We need to implement support for this new feature while keeping the old behaviour working for projects without that feature.
I want the old deference mechanism to stay working even when the new feature is enabled. After all it is quite similar to how the UI deals with it when the feature is enabled.

The core change should be updating the storage api calls related to buckets/tables so that it calls both main and branched versions 
e.g. we need pull in all what's in branch + all that's in main (excluding intersection with branch, e.g. if it exists in branch, the ids come in from a branch)

