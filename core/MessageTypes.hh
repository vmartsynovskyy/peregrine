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

enum WorkType : uint32_t {
  Count = 0,
};

struct WorkTypeMessage {
  enum WorkType work_type;
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
