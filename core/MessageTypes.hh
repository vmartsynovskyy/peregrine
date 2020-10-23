#include <cstdint>

#ifndef MESSAGETYPES_HH
#define MESSAGETYPES_HH

enum MessageType : uint32_t {
  Task = 0,
  CompletedCountTask,
  CompletedMatchTask,
  JobDone,
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
  uint32_t nsets;
};

struct PeregrineMessage {
  MessageType msg_type;
  union {
    struct Task task;
    struct CompletedMatchTask comp_m_task;
    struct CompletedCountTask comp_c_task;
  } msg;
};

#endif // MESSAGETYPES_HH
