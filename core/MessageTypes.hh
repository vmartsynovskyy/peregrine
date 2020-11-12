#include <cstdint>

#ifndef MESSAGETYPES_HH
#define MESSAGETYPES_HH

enum MessageType : uint32_t {
  Task = 0,
  CompletedCountTask,
  CompletedMatchTask,
  JobDone,
  Patterns,
  WorkType,
  StopWorker,
};

enum PgWorkType : uint32_t {
  Count = 0,
  MatchSingle,
  MatchVector,
  MatchMulti,
};

enum AggKeyType : uint32_t {
  Pattern = 0,
};

enum AggValueType : uint32_t {
  Bool = 0,
  Domain,
  DiscoveryDomain,
};

enum ProcessFunc : uint32_t {
  FirstFsm = 0,
  SecondFsm,
  Existence,
};

struct WorkTypeMessage {
  PgWorkType work_type;
  AggKeyType agg_key_type;
  AggValueType agg_value_type;
  uint32_t process_func_idx = 0;
};

struct Task {
  uint64_t task_id;
  uint64_t start_task;
  uint64_t end_task;
  uint32_t pattern_idx;
};

struct CompletedCountTask {
  uint64_t task_id;
  uint32_t pattern_idx;
  uint64_t count;
};

struct CompletedMatchTask {
  uint64_t task_id;
  uint32_t pattern_idx;
};

struct PeregrineMessage {
  MessageType msg_type;
  union {
    struct Task task;
    struct CompletedMatchTask comp_m_task;
    struct CompletedCountTask comp_c_task;
    struct WorkTypeMessage work_type_msg;
  } msg;
};

#endif // MESSAGETYPES_HH
