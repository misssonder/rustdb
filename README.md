# rustdb
Totally async, pure rust implement database based on B+Tree for learning. Refer to [bustub](https://github.com/cmu-db/bustub), [toydb](https://github.com/erikgrinaker/toydb). Which is still in the experimental stage.
## roadmap
- [x] Parsing
  - [x] Begin
  - [x] Commit
  - [x] Rollback
  - [x] CreateTable
  - [x] DropTable
  - [x] Delete
  - [x] Insert
  - [x] Update
  - [x] Select
  - [x] Explain
- [ ] Planner
- [ ] Executor
- [ ] Transaction
- [X] BPlus Tree
  - [x] Search
  - [x] Insert
  - [x] Delete
  - [X] Concurrency
- [x] Buffer Poll
  - [x] Evict Policy 
  - [x] Page lifetime manage 