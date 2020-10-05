#include <cstdint>

#ifndef MESSAGETYPES_HH
#define MESSAGETYPES_HH

struct Task {
  uint32_t start_task;
  uint32_t end_task;
  uint32_t pattern_idx;
};

struct PeregrineMessage {
  int msg_type;
  union {
    struct Task task;
  } msg;
};

#endif // MESSAGETYPES_HH
