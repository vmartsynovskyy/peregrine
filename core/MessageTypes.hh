#include <cstdint>

#ifndef MESSAGETYPES_HH
#define MESSAGETYPES_HH

struct Task {
  uint64_t task_id;
  uint64_t start_task;
  uint64_t end_task;
  uint32_t pattern_idx;
};

struct PeregrineMessage {
  int msg_type;
  union {
    struct Task task;
  } msg;
};

#endif // MESSAGETYPES_HH
