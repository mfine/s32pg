### Run

```
createdb s32pg
export DATABASE_URL=postgres://localhost/s32pg
export AWS_ACCESS_KEY_ID=oil
export AWS_SECRET_ACCESS_KEY=vinegar
psql s32pg
# load db.sql
go build
s32pg --bucket fine-stuff
```