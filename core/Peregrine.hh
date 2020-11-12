#ifndef PEREGRINE_HH
#define PEREGRINE_HH

#include <type_traits>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <algorithm>
#include <bitset>
#include <cstdlib>

#include "Options.hh"
#include "Graph.hh"
#include "PatternGenerator.hh"
#include "PatternMatching.hh"
#include "MessageTypes.hh"

#include "zmq/zmq.hpp"
#include "zmq/zmq_addon.hpp"

#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_map.hpp>

#define CALL_COUNT_LOOP_PARALLEL(L, has_anti_vertices)\
{\
  switch (L)\
  {\
    case Graph::LABELLED:\
      lcount += count_loop_parallel<Graph::LABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::UNLABELLED:\
      lcount += count_loop_parallel<Graph::UNLABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::PARTIALLY_LABELLED:\
      lcount += count_loop_parallel<Graph::PARTIALLY_LABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::DISCOVER_LABELS:\
      lcount += count_loop_parallel<Graph::DISCOVER_LABELS, has_anti_vertices>(dg, cands);\
      break;\
  }\
}

#define CALL_COUNT_LOOP(L, has_anti_vertices)\
{\
  switch (L)\
  {\
    case Graph::LABELLED:\
      lcount += count_loop<Graph::LABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::UNLABELLED:\
      lcount += count_loop<Graph::UNLABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::PARTIALLY_LABELLED:\
      lcount += count_loop<Graph::PARTIALLY_LABELLED, has_anti_vertices>(dg, cands);\
      break;\
    case Graph::DISCOVER_LABELS:\
      lcount += count_loop<Graph::DISCOVER_LABELS, has_anti_vertices>(dg, cands);\
      break;\
  }\
}

#define CALL_MATCH_LOOP(L, has_anti_edges, has_anti_vertices)\
{\
  switch (L)\
  {\
    case Graph::LABELLED:\
      match_loop<Graph::LABELLED, has_anti_edges, has_anti_vertices, OnTheFly, Stoppable>(dg, process, cands, ah);\
      break;\
    case Graph::UNLABELLED:\
      match_loop<Graph::UNLABELLED, has_anti_edges, has_anti_vertices, OnTheFly, Stoppable>(dg, process, cands, ah);\
      break;\
    case Graph::PARTIALLY_LABELLED:\
      match_loop<Graph::PARTIALLY_LABELLED, has_anti_edges, has_anti_vertices, OnTheFly, Stoppable>(dg, process, cands, ah);\
      break;\
    case Graph::DISCOVER_LABELS:\
      match_loop<Graph::DISCOVER_LABELS, has_anti_edges, has_anti_vertices, OnTheFly, Stoppable>(dg, process, cands, ah);\
      break;\
  }\
}

#define CALL_MATCH_LOOP_PARALLEL(L, has_anti_edges, has_anti_vertices)\
{\
  switch (L)\
  {\
    case Graph::LABELLED:\
      match_loop_parallel<Graph::LABELLED, has_anti_edges, has_anti_vertices>(dg, process, cands, ah);\
      break;\
    case Graph::UNLABELLED:\
      match_loop_parallel<Graph::UNLABELLED, has_anti_edges, has_anti_vertices>(dg, process, cands, ah);\
      break;\
    case Graph::PARTIALLY_LABELLED:\
      match_loop_parallel<Graph::PARTIALLY_LABELLED, has_anti_edges, has_anti_vertices>(dg, process, cands, ah);\
      break;\
    case Graph::DISCOVER_LABELS:\
      match_loop_parallel<Graph::DISCOVER_LABELS, has_anti_edges, has_anti_vertices>(dg, process, cands, ah);\
      break;\
  }\
}

namespace Peregrine
{
  // XXX: construct for each application?
  // so that e.g. gcount isn't accessible from a match()
  // or so task_ctr can't be modified at runtime
  namespace Context
  {
    std::shared_ptr<AnalyzedPattern> current_pattern;
    DataGraph *data_graph;
    std::atomic<uint64_t> task_ctr(0);
    uint64_t task_end(0);
    uint32_t pattern_idx(0);
    std::atomic<uint64_t> gcount(0);
    const auto processFirstFsm = [](auto &&a, auto &&cm) {
      uint32_t merge = cm.pattern[1] == cm.pattern[2] ? 1 : 2;
      a.map(cm.pattern, std::make_pair(cm.mapping, merge));
    };
    const auto processSecondFsm = [](auto &&a, auto &&cm) {
      a.map(cm.pattern, cm.mapping);
    };
    const auto processExistence = [](auto &&a, auto &&cm) {
      a.map(cm.pattern, true);
      a.stop();
    };
  }

  struct flag_t { bool on, working; };
  static constexpr flag_t ON() { return {true, false}; }
  static constexpr flag_t WORKING() { return {true, true}; }
  static constexpr flag_t OFF() { return {false, false}; }
}


#include "aggregators/SingleValueAggregator.hh"
#include "aggregators/VectorAggregator.hh"
#include "aggregators/Aggregator.hh"
#include "Barrier.hh"
#include "../apps/Domain.hh"

namespace Peregrine
{
  struct Master {
    std::unique_ptr<zmq::socket_t> sock;
    size_t nworkers;
    zmq::context_t context;
    std::unique_ptr<DataGraph> data_graph;
    std::vector<std::string> worker_ids;
  };

  class TaskIterator
  {
  public:
    uint64_t num_tasks_per_item = 1024;
    class iterator: public std::iterator<std::input_iterator_tag, struct Task>
    {
      const DataGraph &dg;
      const std::vector<SmallGraph> &patterns;
      uint64_t curr_task_id = 0;
      uint64_t task_item_start = 0;
      uint64_t num_tasks_per_item = 1024;
      uint64_t num_tasks = 0;
      size_t pattern_idx = 0;

    public:
      bool end = false;
      explicit iterator(const DataGraph &dg, const std::vector<SmallGraph> &patterns,
                        size_t pattern_idx, uint64_t task_item_start)
        : dg(dg), patterns(patterns), task_item_start(task_item_start), pattern_idx(pattern_idx)
      {
        AnalyzedPattern ap(patterns[pattern_idx]);
        uint32_t vgs_count = ap.vgs.size();
        uint32_t num_vertices = dg.get_vertex_count();
        num_tasks = vgs_count * num_vertices;
      }

      iterator& operator++()
      {
        curr_task_id++;
        task_item_start += num_tasks_per_item;
        if (task_item_start > num_tasks) {
          pattern_idx++;
          if (pattern_idx >= patterns.size()) {
            end = true;
            return *this;
          }
          task_item_start = 0;
          AnalyzedPattern ap(patterns[pattern_idx]);
          uint32_t vgs_count = ap.vgs.size();
          uint32_t num_vertices = dg.get_vertex_count();
          num_tasks = vgs_count * num_vertices;
        }

        return *this;
      }

      value_type operator*() const
      {
        struct Task task {
          .task_id     = curr_task_id,
          .start_task  = task_item_start,
          .end_task    = std::min(task_item_start + num_tasks_per_item, num_tasks + 1),
          .pattern_idx = (uint32_t) pattern_idx,
        };
        return task;
      }
    };

    const DataGraph& dg;
    const std::vector<SmallGraph> &patterns;

    TaskIterator(const DataGraph &dg, const std::vector<SmallGraph> &patterns)
      : dg(dg), patterns(patterns) {}

    iterator begin()
    {
      return iterator(dg, patterns, 0, 0);
    }
  };

  struct Worker {
    std::unique_ptr<zmq::socket_t> sock;
    zmq::context_t context;
    size_t nthreads;
  };

  std::pair<std::unique_ptr<zmq::socket_t>, std::unique_ptr<zmq::socket_t>>
  create_worker_sockets(zmq::context_t &ctx, std::string master_host)
  {
    auto worker_pull_sock = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::pull);
    auto worker_push_sock = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::push);

    worker_pull_sock->set(zmq::sockopt::linger, 0);
    worker_push_sock->set(zmq::sockopt::linger, 0);

    char hostname_buf[HOST_NAME_MAX + 11];

    std::snprintf(hostname_buf, HOST_NAME_MAX + 11, "tcp://%s:9999", master_host.c_str());
    worker_pull_sock->connect(hostname_buf);

    std::snprintf(hostname_buf, HOST_NAME_MAX + 11, "tcp://%s:9998", master_host.c_str());
    worker_push_sock->connect(hostname_buf);

    worker_push_sock->send(zmq::str_buffer("ready"), zmq::send_flags::none);

    return std::pair(std::move(worker_pull_sock), std::move(worker_push_sock));
  }

  std::pair<std::unique_ptr<zmq::socket_t>, std::unique_ptr<zmq::socket_t>>
  create_master_sockets(zmq::context_t &ctx, uint32_t nworkers)
  {
    auto master_pull_sock = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::pull);
    auto master_push_sock = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::push);

    master_pull_sock->set(zmq::sockopt::linger, 0);
    master_push_sock->set(zmq::sockopt::linger, 0);

    master_push_sock->bind("tcp://*:9999");
    master_pull_sock->bind("tcp://*:9998");

    zmq::message_t ready_msg;
    uint32_t num_workers_ready = 0;
    utils::Log{} << "Waiting for all workers to become ready...\n";
    while (num_workers_ready < nworkers) {
      auto res = master_pull_sock->recv(ready_msg, zmq::recv_flags::none);
      if (res.has_value() && strcmp((char*) ready_msg.data(), "ready")) {
        num_workers_ready++;
        utils::Log{} << "[" << num_workers_ready << "/" << nworkers << "] workers are ready" << "\n";
      }
    }

    return std::pair(std::move(master_pull_sock), std::move(master_push_sock));
  }

  bool
  send_task_msg(zmq::socket_t &socket, const std::string &worker_id, struct Task task)
  {
    struct PeregrineMessage message = {
      .msg_type   = MessageType::Task,
      .msg        = {},
    };
    message.msg.task = task;

    utils::Log{} << task.start_task << ":" << task.end_task << " " << task.pattern_idx << "\n";

    zmq::multipart_t zmq_multi_msg;
    zmq_multi_msg.addstr(worker_id);
    zmq_multi_msg.addstr("");

    // send task message with zmq
    zmq::message_t zmq_msg(&message, sizeof(message));
    zmq_multi_msg.add(std::move(zmq_msg));
    return zmq_multi_msg.send(socket, 0);
  }

  zmq::send_result_t
  send_task_msg(zmq::socket_t &socket, uint64_t start_task, uint64_t end_task, uint64_t task_id, uint32_t pattern_idx)
  {
    struct Task task = {
      .task_id        = task_id,
      .start_task     = start_task,
      .end_task       = end_task,
      .pattern_idx    = pattern_idx,
    };
    struct PeregrineMessage message = {
      .msg_type   = MessageType::Task,
      .msg        = {},
    };
    message.msg.task = task;

    // send task message with zmq
    zmq::message_t zmq_msg(&message, sizeof(message));
    return socket.send(zmq_msg, zmq::send_flags::none);
  }

  void stop_workers(Master &master)
  {
    struct PeregrineMessage job_done_pg_msg = {
      .msg_type = MessageType::JobDone,
      .msg = {},
    };
    for (auto worker_id : master.worker_ids) {
      zmq::message_t zmq_job_done_msg(&job_done_pg_msg, sizeof(job_done_pg_msg));
      zmq::multipart_t zmq_multi_msg;
      zmq_multi_msg.addstr(worker_id);
      zmq_multi_msg.addstr("");
      zmq_multi_msg.add(std::move(zmq_job_done_msg));
      zmq_multi_msg.send(*master.sock, 0);
    }

    return;
  }

  void stop_workers(zmq::socket_t& master_pull_sock, zmq::socket_t& master_push_sock, uint32_t nworkers)
  {
    struct PeregrineMessage job_done_pg_msg = {
      .msg_type = MessageType::JobDone,
      .msg = {},
    };
    uint32_t workers_done = 0;
    while (workers_done < nworkers) {
      zmq::message_t zmq_job_done_msg(&job_done_pg_msg, sizeof(job_done_pg_msg));
      master_push_sock.send(zmq_job_done_msg, zmq::send_flags::none);
      if (master_pull_sock.recv(zmq_job_done_msg, zmq::recv_flags::dontwait).has_value()) {
        workers_done++;
      }
    }

    return;
  }

  // for each pattern, calculate the vertex-based count
  std::vector<std::pair<SmallGraph, uint64_t>> convert_counts(std::vector<std::pair<SmallGraph, uint64_t>> edge_based, const std::vector<SmallGraph> &original_patterns)
  {
    std::vector<std::pair<SmallGraph, uint64_t>> vbased(edge_based.size());

    for (int32_t i = edge_based.size()-1; i >= 0; --i) {
      uint64_t count = edge_based[i].second;
      for (uint32_t j = i+1; j < edge_based.size(); ++j) {
        // mapping edge_based[i].first into edge_based[j].first
        uint32_t n = num_mappings(edge_based[j].first, edge_based[i].first);
        uint64_t inc = n * vbased[j].second;
        count -= inc;
      }
      vbased[i] = {original_patterns[i], count};
    }

    return vbased;
  }

  template <Graph::Labelling L,
    bool has_anti_edges,
    bool has_anti_vertices,
    OnTheFlyOption OnTheFly,
    StoppableOption Stoppable,
    typename Func,
    typename HandleType>
  inline void match_loop(DataGraph *dg, const Func &process, std::vector<std::vector<uint32_t>> &cands, HandleType &a)
  {
    (void)a;

    uint32_t vgs_count = dg->get_vgs_count();
    uint32_t num_vertices = dg->get_vertex_count();
    uint64_t num_tasks = num_vertices * vgs_count;

    uint64_t task = 0;
    while ((task = Context::task_ctr.fetch_add(1, std::memory_order_relaxed) + 1) <= num_tasks)
    {
      uint32_t v = (task-1) / vgs_count + 1;
      uint32_t vgsi = task % vgs_count;
      Matcher<has_anti_vertices, Stoppable, decltype(process)> m(dg->rbi, dg, vgsi, cands, process);
      m.template map_into<L, has_anti_edges>(v);

      if constexpr (Stoppable == STOPPABLE)
      {
        pthread_testcancel();
      }

      if constexpr (OnTheFly == ON_THE_FLY)
      {
        a.submit();
      }
    }
  }

  template <Graph::Labelling L, bool has_anti_vertices>
  inline uint64_t count_loop(DataGraph *dg, std::vector<std::vector<uint32_t>> &cands)
  {
    uint32_t vgs_count = dg->get_vgs_count();
    uint32_t num_vertices = dg->get_vertex_count();
    uint64_t num_tasks = num_vertices * vgs_count;

    uint64_t lcount = 0;

    uint64_t task = 0;
    while ((task = Context::task_ctr.fetch_add(1, std::memory_order_relaxed) + 1) <= num_tasks)
    {
      uint32_t v = (task-1) / vgs_count + 1;
      uint32_t vgsi = task % vgs_count;
      Counter<has_anti_vertices> m(dg->rbi, dg, vgsi, cands);
      lcount += m.template map_into<L>(v);
    }

    return lcount;
  }

  template <Graph::Labelling L,
    bool has_anti_edges,
    bool has_anti_vertices,
    typename Func,
    typename HandleType>
  inline void match_loop_parallel(DataGraph *dg, const Func &process, std::vector<std::vector<uint32_t>> &cands, HandleType &a)
  {
    (void)a;

    uint32_t vgs_count = dg->get_vgs_count();

    uint64_t task = 0;
    while ((task = Context::task_ctr.fetch_add(1, std::memory_order_relaxed) + 1) <= Context::task_end)
    {
      uint32_t v = (task-1) / vgs_count + 1;
      uint32_t vgsi = task % vgs_count;
      Matcher<has_anti_vertices, Peregrine::UNSTOPPABLE, decltype(process)> m(dg->rbi, dg, vgsi, cands, process);
      m.template map_into<L, has_anti_edges>(v);
    }
  }

  template <Graph::Labelling L, bool has_anti_vertices>
  inline uint64_t count_loop_parallel(DataGraph *dg, std::vector<std::vector<uint32_t>> &cands)
  {
    uint32_t vgs_count = dg->get_vgs_count();

    uint64_t lcount = 0;

    uint64_t task = 0;
    while ((task = Context::task_ctr.fetch_add(1, std::memory_order_relaxed) + 1) <= Context::task_end)
    {
      uint32_t v = (task-1) / vgs_count + 1;
      uint32_t vgsi = task % vgs_count;
      Counter<has_anti_vertices> m(dg->rbi, dg, vgsi, cands);
      lcount += m.template map_into<L>(v);
    }

    return lcount;
  }

  std::vector<SmallGraph>
  read_patterns(
      const std::string &data_str
  )
  {
    std::vector<SmallGraph> results;
    std::stringstream ss(data_str);
    {
      cereal::BinaryInputArchive archive(ss);
      archive(results);
    }

    for (const auto &p : results)
    {
      std::cout << p << std::endl;
    }

    return results;
  }

  void
  send_patterns_to_workers(
      const Master &master,
      const std::vector<SmallGraph> &patterns
  )
  {
    for (auto &identity : master.worker_ids) {
      zmq::multipart_t multi_msg;
      multi_msg.addstr(identity);
      multi_msg.addstr("");

      struct PeregrineMessage pattern_pg_msg{};
      pattern_pg_msg.msg_type = MessageType::Patterns;
      std::stringstream ss;
      {
        cereal::BinaryOutputArchive archive(ss);

        archive(patterns);
      }
      auto serial_data = ss.str();
      zmq::message_t pattern_msg(sizeof(pattern_pg_msg) + serial_data.length());
      char *msg_buff = static_cast<char*>(pattern_msg.data());
      std::memcpy(msg_buff, &pattern_pg_msg, sizeof(pattern_pg_msg));
      std::memcpy(msg_buff + sizeof(pattern_pg_msg), serial_data.data(), serial_data.size());
      multi_msg.add(std::move(pattern_msg));

      multi_msg.send(*master.sock, 0);
      utils::Log{} << "send pattern\n";
    }
  }

  void
  send_work_type_to_workers(
      const Master &master,
      enum PgWorkType work_type
  )
  {
    for (auto &identity : master.worker_ids) {
      zmq::multipart_t multi_msg;
      multi_msg.addstr(identity);
      multi_msg.addstr("");

      struct PeregrineMessage worktype_pg_msg{};
      worktype_pg_msg.msg_type = MessageType::WorkType;

      struct WorkTypeMessage worktype_msg {work_type};
      worktype_pg_msg.msg.work_type_msg = worktype_msg;
      multi_msg.addmem(&worktype_pg_msg, sizeof(worktype_pg_msg));
      
      multi_msg.send(*master.sock, 0);
    }
  }

  std::tuple<struct PeregrineMessage, std::string, std::string>
  get_pg_msg_data_and_id(
      const Master &master
  )
  {
    zmq::multipart_t multi_msg(*master.sock);
    // data part
    const auto &pg_zmq_msg = multi_msg.remove();
    const PeregrineMessage *pg_msg = static_cast<const PeregrineMessage*>(pg_zmq_msg.data());

    std::string data_str;
    if (pg_zmq_msg.size() > sizeof(PeregrineMessage)) {
      size_t data_str_size = pg_zmq_msg.size() - sizeof(PeregrineMessage);
      data_str.resize(data_str_size);
      std::memcpy(data_str.data(), (char *) pg_zmq_msg.data() + sizeof(PeregrineMessage), data_str_size);
    }

    // envelope delimiter
    multi_msg.remove();

    // identity
    std::string worker_id = multi_msg.remove().to_string();

    return std::tuple(*pg_msg, data_str, worker_id);
  }

  std::pair<struct PeregrineMessage, std::string>
  get_pg_msg_and_data(
      const Worker &worker
  )
  {
    zmq::multipart_t multi_msg(*worker.sock);
    const auto &pg_zmq_msg = multi_msg.back();
    const PeregrineMessage *pg_msg = static_cast<const PeregrineMessage*>(pg_zmq_msg.data());

    std::string data_str;
    if (pg_zmq_msg.size() > sizeof(PeregrineMessage)) {
      size_t data_str_size = pg_zmq_msg.size() - sizeof(PeregrineMessage);
      data_str.resize(data_str_size);
      std::memcpy(data_str.data(), (char *) pg_zmq_msg.data() + sizeof(PeregrineMessage), data_str_size);
    }

    return std::pair(*pg_msg, data_str);
  }

  PgWorkType
  get_work_type(
      const Worker &worker
  )
  {
    zmq::multipart_t multi_msg(*worker.sock);
    const auto &worktype_zmq_msg = multi_msg.back();
    const PeregrineMessage *pg_msg = static_cast<const PeregrineMessage*>(worktype_zmq_msg.data());
    if (pg_msg->msg_type != MessageType::WorkType) {
      return static_cast<PgWorkType>(-1);
    }

    return pg_msg->msg.work_type_msg.work_type;
  }

  std::vector<std::pair<SmallGraph, uint64_t>>
  count_distributed(
      Master &master,
      const std::vector<SmallGraph> &patterns
  )
  {
    // initialize
    std::vector<std::pair<SmallGraph, uint64_t>> results;
    if (patterns.empty()) return results;

    DataGraph *dg = master.data_graph.get();

    // optimize if all unlabelled vertex-induced patterns of a certain size
    // TODO: if a subset is all unlabelled vertex-induced patterns of a certain
    // size it can be optimized too
    uint32_t sz = patterns.front().num_vertices();
    auto is_same_size = [&sz](const SmallGraph &p) {
        return p.num_vertices() == sz && p.num_anti_vertices() == 0;
      };
    auto is_vinduced = [](const SmallGraph &p) {
        uint32_t m = p.num_anti_edges() + p.num_true_edges();
        uint32_t n = p.num_vertices();
        return m == (n*(n-1))/2;
      };
    uint32_t num_possible_topologies[] = {
      0,
      1,
      1,
      2, // size 3
      6, // size 4
      21, // size 5
      112, // size 6
      853, // size 7
      11117, // size 8
      261080, // size 9
    };

    bool must_convert_counts = false;
    std::vector<SmallGraph> new_patterns;
    if (std::all_of(patterns.cbegin(), patterns.cend(), is_same_size)
        && std::all_of(patterns.cbegin(), patterns.cend(), is_vinduced)
        && (sz < 10 && patterns.size() == num_possible_topologies[sz]))
    {
      must_convert_counts = true;
      new_patterns = PatternGenerator::all(sz, PatternGenerator::VERTEX_BASED, PatternGenerator::EXCLUDE_ANTI_EDGES);
    }
    else
    {
      new_patterns.assign(patterns.cbegin(), patterns.cend());
    }

    send_patterns_to_workers(master, new_patterns);

    send_work_type_to_workers(master, PgWorkType::Count);

    std::vector<uint64_t> results_map(new_patterns.size(), 0);

    utils::Log{} << "---- MASTER ----\n";

    auto t1 = utils::get_timestamp();
    // send tasks now that workers are ready
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;

    TaskIterator task_it(*dg, new_patterns);
    auto curr_task_it = task_it.begin();
    uint32_t num_active_tasks_per_worker = 3;
    size_t num_workers = master.worker_ids.size();
    while (tasks_completed < tasks_sent || !curr_task_it.end) {
        std::string worker_id;
        if (tasks_sent < num_workers * num_active_tasks_per_worker) {
          worker_id = master.worker_ids[tasks_sent % num_workers];
        } else {
          auto [pg_msg, data, msg_worker_id] = get_pg_msg_data_and_id(master);
          worker_id = msg_worker_id;
          results_map[pg_msg.msg.comp_c_task.pattern_idx] += pg_msg.msg.comp_c_task.count;
          
          tasks_completed++;
        }
        
        if (!curr_task_it.end) {
          send_task_msg(*master.sock, worker_id, *curr_task_it);

          ++curr_task_it;
          ++tasks_sent;
        }
    }

    utils::Log{} << "master done receiving tasks\n";

    for (size_t i = 0; i < new_patterns.size(); i++) {
      results.emplace_back(new_patterns[i], results_map[i]);
    }

    if (must_convert_counts)
    {
      results = convert_counts(results, patterns);
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(master);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  void count_worker_parallel(unsigned tid, DataGraph *dg, Barrier &b)
  {
    (void)tid; // unused

    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    while (b.hit())
    {
      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      uint64_t lcount = 0;

      if (has_anti_edges)
      {
        const auto process = [&lcount](const CompleteMatch &) { lcount += 1; };
        // dummy
        struct {void submit() {}} ah;

        // TODO anti-edges ruin a lot of optimizations, but not all,
        // is there no way to handle them in Counter?
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_COUNT_LOOP_PARALLEL(L, true);
        }
        else
        {
          CALL_COUNT_LOOP_PARALLEL(L, false);
        }
      }

      Context::gcount += lcount;
    }
  }

  void count_worker(unsigned tid, DataGraph *dg, Barrier &b)
  {
    (void)tid; // unused

    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    while (b.hit())
    {
      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      uint64_t lcount = 0;

      if (has_anti_edges)
      {
        constexpr StoppableOption Stoppable = UNSTOPPABLE;
        constexpr OnTheFlyOption OnTheFly = AT_THE_END;

        const auto process = [&lcount](const CompleteMatch &) { lcount += 1; };
        // dummy
        struct {void submit() {}} ah;

        // TODO anti-edges ruin a lot of optimizations, but not all,
        // is there no way to handle them in Counter?
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_COUNT_LOOP(L, true);
        }
        else
        {
          CALL_COUNT_LOOP(L, false);
        }
      }

      Context::gcount += lcount;
    }
  }

  template <
    typename AggKeyT,
    typename AggValueT,
    typename AggregatorType,
    typename F
  >
  void map_worker_parallel(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    MapAggHandle<AggKeyT, AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };
      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, false);
        }
      }
    }
  }

  template <
    typename AggKeyT,
    typename AggValueT,
    OnTheFlyOption OnTheFly,
    StoppableOption Stoppable,
    typename AggregatorType,
    typename F
  >
  void map_worker(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    MapAggHandle<AggKeyT, AggValueT, OnTheFly, Stoppable, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };
      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, false, false);
        }
      }
    }
  }

  template <
    typename AggValueT,
    typename AggregatorType,
    typename F
  >
  void single_worker_parallel(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    SVAggHandle<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };

      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, false);
        }
      }
      ah.submit();
    }
  }

  template <
    typename AggValueT,
    OnTheFlyOption OnTheFly,
    StoppableOption Stoppable,
    typename AggregatorType,
    typename F
  >
  void single_worker(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    SVAggHandle<AggValueT, OnTheFly, Stoppable, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };

      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, false, false);
        }
      }
    }
  }

  template <
    typename AggValueT,
    typename AggregatorType,
    typename F
  >
  void vector_worker_parallel(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    VecAggHandle<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };

      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP_PARALLEL(L, false, false);
        }
      }
    }
  }

  template <
    typename AggValueT,
    OnTheFlyOption OnTheFly,
    StoppableOption Stoppable,
    typename AggregatorType,
    typename F
  >
  void vector_worker(unsigned tid, DataGraph *dg, Barrier &b, AggregatorType &a, F &&p)
  {
    // an extra pre-allocated cand vector for scratch space, and one for anti-vertex
    std::vector<std::vector<uint32_t>> cands(dg->rbi.query_graph.num_vertices() + 2);

    using ViewFunc = decltype(a.viewer);
    VecAggHandle<AggValueT, OnTheFly, Stoppable, ViewFunc> ah(tid, &a, b);
    a.register_handle(tid, ah);

    while (b.hit())
    {
      ah.reset();
      const auto process = [&ah, &p](const CompleteMatch &cm) { p(ah, cm); };

      Graph::Labelling L = dg->rbi.labelling_type();
      bool has_anti_edges = dg->rbi.has_anti_edges();
      bool has_anti_vertices = !dg->rbi.anti_vertices.empty();

      cands.resize(dg->rbi.query_graph.num_vertices()+2);
      for (auto &cand : cands) {
        cand.clear();
        cand.reserve(10000);
      }

      if (has_anti_edges)
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, true, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, true, false);
        }
      }
      else
      {
        if (has_anti_vertices)
        {
          CALL_MATCH_LOOP(L, false, true);
        }
        else
        {
          CALL_MATCH_LOOP(L, false, false);
        }
      }
    }
  }

  template <typename AggregatorType>
  void aggregator_thread(Barrier &barrier, AggregatorType &agg)
  {
    using namespace std::chrono_literals;

    while (!barrier.finished())
    {
      agg.update();
      std::this_thread::sleep_for(300ms);
    }
  }

  template <typename T>
  struct trivial_wrapper
  {
    trivial_wrapper() : val() {}
    // add always_true to prevent overload collision with other constructor
    trivial_wrapper(size_t num_sets, bool always_true) : val() {}
    trivial_wrapper(T v) : val(v) {}
    trivial_wrapper(char* buf, size_t n)
    {
      memcpy(&val, buf, sizeof(val));
    }

    template<class Archive>
    void serialize(Archive &archive)
    {
      archive(val);
    }

    uint32_t getN()
    {
      return 1;
    }

    trivial_wrapper<T> &operator+=(const trivial_wrapper<T> &other) { val += other.val; return *this; }
    void reset() { val = T(); }
    T val;
  };

  template <typename T>
  T default_viewer(T &&v) { return v; }
  template <typename T>
  T null_viewer(T v) { return v; }

  template <
    typename AggKeyT,
    typename GivenAggValueT,
    OnTheFlyOption OnTheFly,
    StoppableOption Stoppable,
    typename DataGraphT,
    typename PF,
    typename VF = decltype(default_viewer<GivenAggValueT>)
  >
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<GivenAggValueT>()))>>
  match(DataGraphT &&data_graph,
      const std::vector<SmallGraph> &patterns,
      size_t nworkers,
      PF &&process,
      VF viewer = default_viewer<GivenAggValueT>)
  {
    if (patterns.empty())
    {
      return {};
    }

    // automatically wrap trivial types so they have .reset() etc
    constexpr bool should_be_wrapped = std::is_trivial<GivenAggValueT>::value;
    using AggValueT = typename std::conditional<should_be_wrapped,
      trivial_wrapper<GivenAggValueT>, GivenAggValueT>::type;
    auto view = [&viewer](auto &&v)
    {
      if constexpr (should_be_wrapped)
      {
        return viewer(std::move(v.val));
      }
      else
      {
        return viewer(v);
      }
    };

    // optimize AggKeyT == Pattern
    if constexpr (std::is_same_v<AggKeyT, Pattern>)
    {
      std::vector<SmallGraph> single;
      std::vector<SmallGraph> vector;
      std::vector<SmallGraph> multi;

      for (const auto &p : patterns)
      {
        Graph::Labelling l = p.get_labelling();
        switch (l)
        {
          case Graph::LABELLED:
          case Graph::UNLABELLED:
            single.emplace_back(p);
            break;
          case Graph::PARTIALLY_LABELLED:
            vector.emplace_back(p);
            break;
          case Graph::DISCOVER_LABELS:
            multi.emplace_back(p);
            break;
        }
      }

      if (!single.empty()
          && std::is_integral_v<GivenAggValueT>
          && Stoppable == UNSTOPPABLE && OnTheFly == AT_THE_END)
      {
        utils::Log{}
          << "WARN: If you are counting, Peregrine::count() is much faster!"
          << "\n";
      }

      auto result = match_single<AggValueT, OnTheFly, Stoppable>(process, view, nworkers, data_graph, single);
      auto vector_result = match_vector<AggValueT, OnTheFly, Stoppable>(process, view, nworkers, data_graph, vector);
      auto multi_result = match_multi<AggKeyT, AggValueT, OnTheFly, Stoppable>(process, view, nworkers, data_graph, multi);

      result.insert(result.end(), vector_result.begin(), vector_result.end());
      result.insert(result.end(), multi_result.begin(), multi_result.end());

      return result;
    }
    else
    {
      return match_multi<AggKeyT, AggValueT, OnTheFly, Stoppable>(process, view, nworkers, data_graph, patterns);
    }
  }

  template <
    typename AggKeyT,
    typename GivenAggValueT,
    typename DataGraphT,
    typename PF,
    typename VF = decltype(default_viewer<GivenAggValueT>)
  >
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<GivenAggValueT>()))>>
  match_distributed(
      Master &master,
      const std::vector<SmallGraph> &patterns,
      PF &&process,
      VF viewer = default_viewer<GivenAggValueT>)
  {
    if (patterns.empty())
    {
      return {};
    }

    // automatically wrap trivial types so they have .reset() etc
    constexpr bool should_be_wrapped = std::is_trivial<GivenAggValueT>::value;
    using AggValueT = typename std::conditional<should_be_wrapped,
      trivial_wrapper<GivenAggValueT>, GivenAggValueT>::type;
    auto view = [&viewer](auto &&v)
    {
      if constexpr (should_be_wrapped)
      {
        return viewer(std::move(v.val));
      }
      else
      {
        return viewer(v);
      }
    };

    // optimize AggKeyT == Pattern
    if constexpr (std::is_same_v<AggKeyT, Pattern>)
    {
      std::vector<SmallGraph> single;
      std::vector<SmallGraph> vector;
      std::vector<SmallGraph> multi;

      for (const auto &p : patterns)
      {
        Graph::Labelling l = p.get_labelling();
        switch (l)
        {
          case Graph::LABELLED:
          case Graph::UNLABELLED:
            single.emplace_back(p);
            break;
          case Graph::PARTIALLY_LABELLED:
            vector.emplace_back(p);
            break;
          case Graph::DISCOVER_LABELS:
            multi.emplace_back(p);
            break;
        }
      }

      if (!single.empty()
          && std::is_integral_v<GivenAggValueT>)
      {
        utils::Log{}
          << "WARN: If you are counting, Peregrine::count() is much faster!"
          << "\n";
      }

      auto result = match_single_distributed<AggValueT>(master, viewer, single);
      auto vector_result = match_vector_distributed<AggValueT>(master, viewer, vector);
      auto multi_result = match_multi_parallel<AggKeyT, AggValueT>(master, viewer, multi);

      result.insert(result.end(), vector_result.begin(), vector_result.end());
      result.insert(result.end(), multi_result.begin(), multi_result.end());

      return result;
    }
    else
    {
      return match_multi_parallel<AggKeyT, AggValueT>(master, viewer, patterns);
    }
  }

  template <typename AggKeyT, typename AggValueT, OnTheFlyOption OnTheFly, StoppableOption Stoppable, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_multi
  (PF &&process, VF &&viewer, size_t nworkers, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    // initialize
    Barrier barrier(nworkers);
    std::vector<std::thread> pool;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    MapAggregator<AggKeyT, AggValueT, OnTheFly, Stoppable, decltype(viewer)> aggregator(nworkers, viewer);

    for (uint8_t i = 0; i < nworkers; ++i)
    {
      //auto &ah = aggregator.get_handle(i);
      pool.emplace_back(map_worker<
            AggKeyT,
            AggValueT,
            OnTheFly,
            Stoppable,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    std::thread agg_thread;
    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread = std::thread(aggregator_thread<decltype(aggregator)>, std::ref(barrier), std::ref(aggregator));
    }

    // make sure the threads are all running
    barrier.join();

    auto t1 = utils::get_timestamp();
    for (const auto &p : patterns)
    {
      // reset state
      Context::task_ctr = 0;

      // set new pattern
      dg.set_rbi(p);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));
      // prepare handles for the next pattern
      aggregator.reset();

      // begin matching
      barrier.release();

      // sleep until matching finished
      bool called_stop = barrier.join();

      aggregator.get_result();

      if constexpr (Stoppable == STOPPABLE)
      {
        if (called_stop)
        {
          // cancel
          for (auto &th : pool)
          {
            pthread_cancel(th.native_handle());
          }

          // wait for them all to end
          for (auto &th : pool)
          {
            th.join();
          }

          pool.clear();
          barrier.reset();

          // restart workers
          for (uint8_t i = 0; i < nworkers; ++i)
          {
            pool.emplace_back(map_worker<
                  AggKeyT,
                  AggValueT,
                  OnTheFly,
                  Stoppable,
                  decltype(aggregator),
                  PF
                >,
                i,
                &dg,
                std::ref(barrier),
                std::ref(aggregator),
                std::ref(process));
          }

          barrier.join();
        }
      }

      for (auto &[k, v] : aggregator.latest_result)
      {
        results.emplace_back(SmallGraph(p, k), v);
      }
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  template <typename AggKeyT,
           typename AggValueT,
           typename DataGraphT,
           typename PF,
           typename VF>
  void
  match_multi_distributed_worker(
      struct Worker& worker,
      DataGraph &dg,
      const std::vector<SmallGraph> &patterns,
      PF &&process)
  {
    // initialize
    Barrier barrier(worker.nthreads);
    std::vector<std::thread> pool;
    dg.set_rbi(patterns.front());


    auto viewer = default_viewer<AggValueT>;
    MapAggregator<AggKeyT, AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(worker.nthreads, viewer);

    for (uint8_t i = 0; i < worker.nthreads; ++i)
    {
      pool.emplace_back(map_worker_parallel<
            AggValueT,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    // make sure the threads are all running
    barrier.join();

    utils::timestamp_t working_t_sum = 0;
    auto t1 = utils::get_timestamp();
    uint64_t worker_tasks_completed = 0;
    for (;;) {
      auto [task_pg_msg, data] = get_pg_msg_and_data(worker);
      if (task_pg_msg.msg_type == MessageType::JobDone) {
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg.msg.task.start_task;
      Context::task_end = task_pg_msg.msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg.msg.task.pattern_idx;
      dg.set_rbi(patterns[Context::pattern_idx]);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

      // prepare handles for the next pattern
      aggregator.reset();

      barrier.release();

      barrier.join();

      // get thread-local values
      aggregator.get_result();

      auto working_t2 = utils::get_timestamp();
      working_t_sum += (working_t2 - working_t1);

      send_match_task_complete_msg(*worker.sock, task_pg_msg.msg.task, aggregator.global);
      worker_tasks_completed++;
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename AggValueT, typename DataGraphT, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_vector_distributed(
      Master &master,
      VF &&viewer,
      const std::vector<SmallGraph> &patterns
  )
  {
    // initialize
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;
    if (patterns.empty()) return results;

    DataGraph *dg = master.data_graph.get();

    send_patterns_to_workers(master, patterns);
    send_work_type_to_workers(master, PgWorkType::MatchVector);

    utils::Log{} << "---- MASTER ----\n";
    auto t1 = utils::get_timestamp();

    // calculated same way as in VecAggregator
    uint32_t vec_agg_offset = dg->get_label_range().first;
    uint32_t vec_agg_size = dg->get_label_range().second - vec_agg_offset + 1;
    std::vector<std::vector<AggValueT>> results_map;
    for (auto &p : patterns) {
      AnalyzedPattern ap(p);
      std::vector<AggValueT> inner_results_map;
      for (uint32_t j = 0; j < vec_agg_size; j++) {
        inner_results_map.emplace_back(ap.num_aut_sets(), true);
      }
      results_map.push_back(inner_results_map);
    }

    // send tasks now that workers are ready
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    TaskIterator task_it(*dg, patterns);
    auto curr_task_it = task_it.begin();
    uint32_t num_active_tasks_per_worker = 3;
    size_t num_workers = master.worker_ids.size();
    while (tasks_completed < tasks_sent || !curr_task_it.end) {
        std::string worker_id;
        if (tasks_sent < num_workers * num_active_tasks_per_worker) {
          worker_id = master.worker_ids[tasks_sent % num_workers];
        } else {
          auto [pg_msg, data, msg_worker_id] = get_pg_msg_data_and_id(master);
          worker_id = msg_worker_id;
          uint32_t pattern_idx = pg_msg.msg.comp_m_task.pattern_idx;

          std::vector<AggValueT> agg_result;
          std::stringstream ss(data);
          {
            cereal::BinaryInputArchive archive(ss);
            archive(agg_result);
          }

          auto pattern_results_map = results_map[pattern_idx];
          for (size_t i = 0; i < pattern_results_map.size(); i++) {
            pattern_results_map[i] += agg_result[i];
          }

          tasks_completed++;
        }
        
        if (!curr_task_it.end) {
          send_task_msg(*master.sock, worker_id, *curr_task_it);

          ++curr_task_it;
          ++tasks_sent;
        }
    }

    for (size_t i = 0; i < patterns.size(); i++) {
      auto p = patterns[i];
      std::vector<uint32_t> ls(p.get_labels().cbegin(), p.get_labels().cend());
      dg->set_rbi(p);
      uint32_t pl = dg->new_label;
      uint32_t l = 0;
      for (auto &m : results_map[i])
      {
        ls[pl] = l;
        results.emplace_back(SmallGraph(p, ls), viewer(m));
        l += 1;
      }
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(master);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  template <typename AggValueT, OnTheFlyOption OnTheFly, StoppableOption Stoppable, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_single
  (PF &&process, VF &&viewer, size_t nworkers, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    // initialize
    Barrier barrier(nworkers);
    std::vector<std::thread> pool;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    SVAggregator<AggValueT, OnTheFly, Stoppable, decltype(viewer)> aggregator(nworkers, viewer);

    for (uint8_t i = 0; i < nworkers; ++i)
    {
      pool.emplace_back(single_worker<
            AggValueT,
            OnTheFly,
            Stoppable,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    std::thread agg_thread;
    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread = std::thread(aggregator_thread<decltype(aggregator)>, std::ref(barrier), std::ref(aggregator));
    }

    // make sure the threads are all running
    barrier.join();

    auto t1 = utils::get_timestamp();
    for (const auto &p : patterns)
    {
      // reset state
      Context::task_ctr = 0;

      // set new pattern
      dg.set_rbi(p);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));
      // prepare handles for the next pattern
      aggregator.reset();

      // begin matching
      barrier.release();

      // sleep until matching finished
      bool called_stop = barrier.join();

      // need to get thread-local values before killing threads
      aggregator.get_result();
      results.emplace_back(p, aggregator.latest_result.load());

      if constexpr (Stoppable == STOPPABLE)
      {
        if (called_stop)
        {
          // cancel
          for (auto &th : pool)
          {
            pthread_cancel(th.native_handle());
          }

          // wait for them all to end
          for (auto &th : pool)
          {
            th.join();
          }

          pool.clear();
          barrier.reset();

          // restart workers
          for (uint8_t i = 0; i < nworkers; ++i)
          {
            pool.emplace_back(single_worker<
                  AggValueT,
                  OnTheFly,
                  Stoppable,
                  decltype(aggregator),
                  PF
                >,
                i,
                &dg,
                std::ref(barrier),
                std::ref(aggregator),
                std::ref(process));
          }

          barrier.join();
        }
      }
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  bool
  send_task_complete_msg(zmq::socket_t &socket, struct Task *task, uint64_t count)
  {
    struct CompletedCountTask completed_task = {
      .task_id      = task->task_id,
      .pattern_idx  = task->pattern_idx,
      .count        = count,
    };
    struct PeregrineMessage pg_completed_task_msg = {
      .msg_type = MessageType::CompletedCountTask,
      .msg      = {},
    };
    pg_completed_task_msg.msg.comp_c_task = completed_task;

    zmq::multipart_t multi_zmq_msg;
    multi_zmq_msg.addstr("");

    zmq::message_t zmq_msg(&pg_completed_task_msg, sizeof(pg_completed_task_msg));
    multi_zmq_msg.add(std::move(zmq_msg));
    return multi_zmq_msg.send(socket, 0);
  }


  template <typename AggKeyT, typename AggValueT>
  bool
  send_match_task_complete_msg(zmq::socket_t &socket, struct Task *task, std::unordered_map<AggKeyT, AggValueT> &agg_value)
  {
    struct CompletedMatchTask completed_task = {
      .task_id      = task->task_id,
      .pattern_idx  = task->pattern_idx,
    };
    struct PeregrineMessage pg_completed_task_msg = {
      .msg_type = MessageType::CompletedMatchTask,
      .msg      = {},
    };
    pg_completed_task_msg.msg.comp_m_task = completed_task;

    std::stringstream ss;
    {
      cereal::BinaryOutputArchive archive(ss);

      archive(agg_value);
    }
    auto serial_data = ss.str();
    zmq::message_t zmq_msg(sizeof(pg_completed_task_msg) + serial_data.length());
    char *msg_buff = static_cast<char*>(zmq_msg.data());
    std::memcpy(msg_buff, &pg_completed_task_msg, sizeof(pg_completed_task_msg));
    std::memcpy(msg_buff + sizeof(pg_completed_task_msg), serial_data.data(), serial_data.size());

    zmq::multipart_t multi_zmq_msg;
    multi_zmq_msg.addstr("");
    multi_zmq_msg.add(std::move(zmq_msg));
    return multi_zmq_msg.send(socket, 0);
  }

  template <typename AggValueT>
  bool
  send_match_task_complete_msg(zmq::socket_t &socket, struct Task *task, std::vector<AggValueT> &agg_value)
  {
    struct CompletedMatchTask completed_task = {
      .task_id      = task->task_id,
      .pattern_idx  = task->pattern_idx,
    };
    struct PeregrineMessage pg_completed_task_msg = {
      .msg_type = MessageType::CompletedMatchTask,
      .msg      = {},
    };
    pg_completed_task_msg.msg.comp_m_task = completed_task;

    std::stringstream ss;
    {
      cereal::BinaryOutputArchive archive(ss);

      archive(agg_value);
    }
    auto serial_data = ss.str();
    zmq::message_t zmq_msg(sizeof(pg_completed_task_msg) + serial_data.length());
    char *msg_buff = static_cast<char*>(zmq_msg.data());
    std::memcpy(msg_buff, &pg_completed_task_msg, sizeof(pg_completed_task_msg));
    std::memcpy(msg_buff + sizeof(pg_completed_task_msg), serial_data.data(), serial_data.size());

    zmq::multipart_t multi_zmq_msg;
    multi_zmq_msg.addstr("");
    multi_zmq_msg.add(std::move(zmq_msg));
    return multi_zmq_msg.send(socket, 0);
  }

  template <typename AggValueT>
  bool
  send_match_task_complete_msg(zmq::socket_t &socket, struct Task *task, AggValueT &agg_value)
  {
    struct CompletedMatchTask completed_task = {
      .task_id      = task->task_id,
      .pattern_idx  = task->pattern_idx,
    };
    struct PeregrineMessage pg_completed_task_msg = {
      .msg_type = MessageType::CompletedMatchTask,
      .msg      = {},
    };
    pg_completed_task_msg.msg.comp_m_task = completed_task;

    std::stringstream ss;
    {
      cereal::BinaryOutputArchive archive(ss);

      archive(agg_value);
    }
    auto serial_data = ss.str();
    zmq::message_t zmq_msg(sizeof(pg_completed_task_msg) + serial_data.length());
    char *msg_buff = static_cast<char*>(zmq_msg.data());
    std::memcpy(msg_buff, &pg_completed_task_msg, sizeof(pg_completed_task_msg));
    std::memcpy(msg_buff + sizeof(pg_completed_task_msg), serial_data.data(), serial_data.size());

    zmq::multipart_t multi_zmq_msg;
    multi_zmq_msg.addstr("");
    multi_zmq_msg.add(std::move(zmq_msg));
    return multi_zmq_msg.send(socket, 0);
  }

  template <typename GivenAggValueT,
            typename PF,
            typename VF = decltype(default_viewer<GivenAggValueT>)>
  void
  match_single_distributed_worker(
      struct Worker& worker,
      DataGraph &dg,
      const std::vector<SmallGraph> &patterns,
      PF &&process,
      VF viewer = default_viewer<GivenAggValueT>)
  {
    constexpr bool should_be_wrapped = std::is_trivial<GivenAggValueT>::value;
    using AggValueT = typename std::conditional<should_be_wrapped,
      trivial_wrapper<GivenAggValueT>, GivenAggValueT>::type;
    // initialize
    Barrier barrier(worker.nthreads);
    std::vector<std::thread> pool;
    dg.set_rbi(patterns.front());

    SVAggregator<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(worker.nthreads, viewer);

    for (uint8_t i = 0; i < worker.nthreads; ++i)
    {
      pool.emplace_back(single_worker_parallel<
            AggValueT,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    // make sure the threads are all running
    barrier.join();

    utils::timestamp_t working_t_sum = 0;
    auto t1 = utils::get_timestamp();
    uint64_t worker_tasks_completed = 0;
    for (;;) {
      auto [task_pg_msg, data] = get_pg_msg_and_data(worker);
      if (task_pg_msg.msg_type == MessageType::JobDone) {
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg.msg.task.start_task;
      Context::task_end = task_pg_msg.msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg.msg.task.pattern_idx;
      dg.set_rbi(patterns[Context::pattern_idx]);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

      // prepare handles for the next pattern
      aggregator.reset();

      barrier.release();

      barrier.join();

      // get thread-local values
      aggregator.get_result();

      auto working_t2 = utils::get_timestamp();
      working_t_sum += (working_t2 - working_t1);

      send_match_task_complete_msg(*worker.sock, &task_pg_msg.msg.task, aggregator.global);
      worker_tasks_completed++;
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename AggValueT, typename DataGraphT, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_single_distributed(
      Master &master,
      VF &&viewer,
      const std::vector<SmallGraph> &patterns
  )
  {
    // initialize
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;
    if (patterns.empty()) return results;

    DataGraph *dg = master.data_graph.get();

    send_patterns_to_workers(master, patterns);
    send_work_type_to_workers(master, PgWorkType::MatchSingle);

    utils::Log{} << "---- MASTER ----\n";
    auto t1 = utils::get_timestamp();

    std::vector<AggValueT> results_map;
    for (auto &p : patterns) {
      AnalyzedPattern ap(p);
      results_map.emplace_back(ap.num_aut_sets(), true);
    }

    // send tasks now that workers are ready
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    TaskIterator task_it(*dg, patterns);
    auto curr_task_it = task_it.begin();
    uint32_t num_active_tasks_per_worker = 3;
    size_t num_workers = master.worker_ids.size();
    while (tasks_completed < tasks_sent || !curr_task_it.end) {
        std::string worker_id;
        if (tasks_sent < num_workers * num_active_tasks_per_worker) {
          worker_id = master.worker_ids[tasks_sent % num_workers];
        } else {
          auto [pg_msg, data, msg_worker_id] = get_pg_msg_data_and_id(master);
          worker_id = msg_worker_id;
          uint32_t pattern_idx = pg_msg.msg.comp_m_task.pattern_idx;

          AggValueT agg_result;
          std::stringstream ss(data);
          {
            cereal::BinaryInputArchive archive(ss);
            archive(agg_result);
          }

          results_map[pattern_idx] += agg_result;

          tasks_completed++;
        }
        
        if (!curr_task_it.end) {
          send_task_msg(*master.sock, worker_id, *curr_task_it);

          ++curr_task_it;
          ++tasks_sent;
        }
    }

    for (size_t i = 0; i < patterns.size(); i++) {
      auto p = patterns[i];
      results.emplace_back(p, viewer(results_map[i]));
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(master);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  template <typename AggValueT, typename DataGraphT, typename PF, typename VF>
  void
  match_vector_distributed_worker(
      struct Worker& worker,
      DataGraph &dg,
      const std::vector<SmallGraph> &patterns,
      PF &&process)
  {
    // initialize
    Barrier barrier(worker.nthreads);
    std::vector<std::thread> pool;
    dg.set_rbi(patterns.front());

    auto viewer = default_viewer<AggValueT>;
    VecAggregator<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(worker.nthreads, viewer);

    for (uint8_t i = 0; i < worker.nthreads; ++i)
    {
      pool.emplace_back(vector_worker_parallel<
            AggValueT,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    // make sure the threads are all running
    barrier.join();

    utils::timestamp_t working_t_sum = 0;
    auto t1 = utils::get_timestamp();
    uint64_t worker_tasks_completed = 0;
    for (;;) {
      auto [task_pg_msg, data] = get_pg_msg_and_data(worker);
      if (task_pg_msg.msg_type == MessageType::JobDone) {
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg.msg.task.start_task;
      Context::task_end = task_pg_msg.msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg.msg.task.pattern_idx;
      dg.set_rbi(patterns[Context::pattern_idx]);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

      // prepare handles for the next pattern
      aggregator.reset();

      barrier.release();

      barrier.join();

      // get thread-local values
      aggregator.get_result();

      auto working_t2 = utils::get_timestamp();
      working_t_sum += (working_t2 - working_t1);

      send_match_task_complete_msg(*worker.sock, task_pg_msg.msg.task, aggregator.global);
      worker_tasks_completed++;
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename AggKeyT, typename AggValueT, typename DataGraphT, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_multi_distributed(
      Master &master,
      VF &&viewer,
      const std::vector<SmallGraph> &patterns
  )
  {
    // initialize
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;
    if (patterns.empty()) return results;

    DataGraph *dg = master.data_graph.get();

    send_patterns_to_workers(master, patterns);
    send_work_type_to_workers(master, PgWorkType::MatchMulti);

    utils::Log{} << "---- MASTER ----\n";
    auto t1 = utils::get_timestamp();

    std::vector<std::unordered_map<AggKeyT, AggValueT>> results_map(patterns.size());

    // send tasks now that workers are ready
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    TaskIterator task_it(*dg, patterns);
    auto curr_task_it = task_it.begin();
    uint32_t num_active_tasks_per_worker = 3;
    size_t num_workers = master.worker_ids.size();
    while (tasks_completed < tasks_sent || !curr_task_it.end) {
        std::string worker_id;
        if (tasks_sent < num_workers * num_active_tasks_per_worker) {
          worker_id = master.worker_ids[tasks_sent % num_workers];
        } else {
          auto [pg_msg, data, msg_worker_id] = get_pg_msg_data_and_id(master);
          worker_id = msg_worker_id;
          uint32_t pattern_idx = pg_msg.msg.comp_m_task.pattern_idx;

          std::unordered_map<AggKeyT, AggValueT> agg_result;
          std::stringstream ss(data);
          {
            cereal::BinaryInputArchive archive(ss);
            archive(agg_result);
          }

          auto pattern_results_map = results_map[pattern_idx];
          for (auto &[k, v] : agg_result) {
            auto results_map_val = pattern_results_map.find(k);
            if (results_map_val == pattern_results_map.end()) {
              pattern_results_map[k] = v;
            } else {
              results_map_val->second += v;
            }
          }

          tasks_completed++;
        }
        
        if (!curr_task_it.end) {
          send_task_msg(*master.sock, worker_id, *curr_task_it);

          ++curr_task_it;
          ++tasks_sent;
        }
    }

    for (size_t i = 0; i < patterns.size(); i++) {
      auto pattern_results_map = results_map[i];
      auto pattern = patterns[i];
      for (auto &[k, v] : pattern_results_map) {
        results.emplace_back(SmallGraph(pattern, k), viewer(v));
      }
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(master);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }

  template <typename AggValueT, OnTheFlyOption OnTheFly, StoppableOption Stoppable, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_vector
  (PF &&process, VF &&viewer, size_t nworkers, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    utils::Log{} << "matching vector\n";

    // initialize
    Barrier barrier(nworkers);
    std::vector<std::thread> pool;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    VecAggregator<AggValueT, OnTheFly, Stoppable, decltype(viewer)> aggregator(nworkers, viewer);

    for (uint8_t i = 0; i < nworkers; ++i)
    {
      //auto &ah = aggregator.get_handle(i);
      pool.emplace_back(vector_worker<
            AggValueT,
            OnTheFly,
            Stoppable,
            decltype(aggregator),
            PF
          >,
          i,
          &dg,
          std::ref(barrier),
          std::ref(aggregator),
          std::ref(process));
    }

    std::thread agg_thread;
    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread = std::thread(aggregator_thread<decltype(aggregator)>, std::ref(barrier), std::ref(aggregator));
    }

    // make sure the threads are all running
    barrier.join();

    auto t1 = utils::get_timestamp();
    for (const auto &p : patterns)
    {
      // reset state
      Context::task_ctr = 0;

      // set new pattern
      dg.set_rbi(p);
      Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));
      // prepare handles for the next pattern
      aggregator.reset();

      // begin matching
      barrier.release();

      // sleep until matching finished
      bool called_stop = barrier.join();

      aggregator.get_result();

      if constexpr (Stoppable == STOPPABLE)
      {
        if (called_stop)
        {
          // cancel
          for (auto &th : pool)
          {
            pthread_cancel(th.native_handle());
          }

          // wait for them all to end
          for (auto &th : pool)
          {
            th.join();
          }

          pool.clear();
          barrier.reset();

          // restart workers
          for (uint8_t i = 0; i < nworkers; ++i)
          {
            pool.emplace_back(vector_worker<
                  AggValueT,
                  OnTheFly,
                  Stoppable,
                  decltype(aggregator),
                  PF
                >,
                i,
                &dg,
                std::ref(barrier),
                std::ref(aggregator),
                std::ref(process));
          }

          barrier.join();
        }
      }

      std::vector<uint32_t> ls(p.get_labels().cbegin(), p.get_labels().cend());
      uint32_t pl = dg.new_label;
      uint32_t l = 0;
      for (auto &m : aggregator.latest_result)
      {
        ls[pl] = l;
        results.emplace_back(SmallGraph(p, ls), m.load());
        l += 1;
      }

    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    if constexpr (OnTheFly == ON_THE_FLY)
    {
      agg_thread.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
  }


  void
  count_distributed_worker(struct Worker& worker, DataGraph &dg, const std::vector<SmallGraph> &patterns)
  {
    Barrier barrier(worker.nthreads);
    std::vector<std::thread> pool;
    dg.set_rbi(patterns.front());
    dg.set_known_labels(patterns);

    for (uint8_t i = 0; i < worker.nthreads; ++i)
    {
      pool.emplace_back(count_worker_parallel,
          i,
          &dg,
          std::ref(barrier));
    }

    // make sure the threads are all running
    barrier.join();

    utils::timestamp_t working_t_sum = 0;
    auto t1 = utils::get_timestamp();
    uint64_t worker_tasks_completed = 0;
    for (;;) {
      auto [task_pg_msg, data] = get_pg_msg_and_data(worker);
      if (task_pg_msg.msg_type == MessageType::JobDone) {
        break;
      }

      auto working_t1 = utils::get_timestamp();
      Context::gcount = 0;

      Context::task_ctr = task_pg_msg.msg.task.start_task;
      Context::task_end = task_pg_msg.msg.task.end_task;
      Context::pattern_idx = task_pg_msg.msg.task.pattern_idx;
      dg.set_rbi(patterns[Context::pattern_idx]);

      barrier.release();

      barrier.join();

      uint64_t global_count = Context::gcount;
      auto working_t2 = utils::get_timestamp();
      working_t_sum += (working_t2 - working_t1);

      send_task_complete_msg(*worker.sock, &task_pg_msg.msg.task, global_count);
      worker_tasks_completed++;
    }
    utils::Log{} << "worker is done all tasks in queue after completing " << worker_tasks_completed << " tasks (" << working_t_sum/1e6 << "s of working time)\n";

    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename DataGraphT>
  std::vector<std::pair<SmallGraph, uint64_t>>
  count(DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns, size_t nworkers)
  {
    // initialize
    std::vector<std::pair<SmallGraph, uint64_t>> results;
    if (patterns.empty()) return results;

    // optimize if all unlabelled vertex-induced patterns of a certain size
    // TODO: if a subset is all unlabelled vertex-induced patterns of a certain
    // size it can be optimized too
    uint32_t sz = patterns.front().num_vertices();
    auto is_same_size = [&sz](const SmallGraph &p) {
        return p.num_vertices() == sz && p.num_anti_vertices() == 0;
      };
    auto is_vinduced = [](const SmallGraph &p) {
        uint32_t m = p.num_anti_edges() + p.num_true_edges();
        uint32_t n = p.num_vertices();
        return m == (n*(n-1))/2;
      };
    uint32_t num_possible_topologies[] = {
      0,
      1,
      1,
      2, // size 3
      6, // size 4
      21, // size 5
      112, // size 6
      853, // size 7
      11117, // size 8
      261080, // size 9
    };

    bool must_convert_counts = false;
    std::vector<SmallGraph> new_patterns;
    if (std::all_of(patterns.cbegin(), patterns.cend(), is_same_size)
        && std::all_of(patterns.cbegin(), patterns.cend(), is_vinduced)
        && (sz < 10 && patterns.size() == num_possible_topologies[sz]))
    {
      must_convert_counts = true;
      new_patterns = PatternGenerator::all(sz, PatternGenerator::VERTEX_BASED, PatternGenerator::EXCLUDE_ANTI_EDGES);
    }
    else
    {
      new_patterns.assign(patterns.cbegin(), patterns.cend());
    }

    Barrier barrier(nworkers);
    std::vector<std::thread> pool;
    DataGraph dg(data_graph);

    dg.set_rbi(new_patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(new_patterns);

    for (uint8_t i = 0; i < nworkers; ++i)
    {
      pool.emplace_back(count_worker,
          i,
          &dg,
          std::ref(barrier));
    }

    // make sure the threads are all running
    barrier.join();

    auto t1 = utils::get_timestamp();
    for (const auto &p : new_patterns)
    {
      // reset state
      Context::task_ctr = 0;
      Context::gcount = 0;
      Context::task_end = dg.get_vgs_count() * dg.get_vertex_count();

      // set new pattern
      dg.set_rbi(p);

      // begin matching
      barrier.release();

      // sleep until matching finished
      barrier.join();

      // get counts
      uint64_t global_count = Context::gcount;
      results.emplace_back(p, global_count);
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    if (must_convert_counts)
    {
      results = convert_counts(results, patterns);
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";


    return results;
  }

  template <typename DataGraphT>
  void
  start_worker(DataGraphT &&data_graph, size_t nthreads, std::string master_host)
  {
    struct Worker worker;
    DataGraph dg(data_graph);
    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    zmq::context_t ctx;
    worker.context = std::move(ctx);
    worker.nthreads = nthreads;

    worker.sock = std::make_unique<zmq::socket_t>(worker.context, zmq::socket_type::dealer);
    worker.sock->set(zmq::sockopt::linger, 0);

    worker.sock->connect("tcp://127.0.0.1:9999");

    zmq::multipart_t ready_msg;
    ready_msg.pushstr("");
    ready_msg.pushstr("ready");
    const auto res = ready_msg.send(*worker.sock, 0);
    if (res) {
      utils::Log{} << "Worker is ready. Waiting for tasks from master...\n";
    }

    std::vector<SmallGraph> patterns;
    for (;;) {
      auto [pg_msg, data_str] = get_pg_msg_and_data(worker);
      if (pg_msg.msg_type == MessageType::Patterns) {
        patterns = read_patterns(data_str);
      } else if (pg_msg.msg_type == MessageType::WorkType) {
        PgWorkType work_type = pg_msg.msg.work_type_msg.work_type;
        WorkTypeMessage work_type_msg = pg_msg.msg.work_type_msg;
        if (work_type == PgWorkType::Count) {
          count_distributed_worker(worker, dg, patterns);
        } else {
          const auto process = Context::processExistence;
          if (work_type == PgWorkType::MatchSingle) {
            if (work_type_msg.agg_value_type == AggValueType::Bool) {
              match_single_distributed_worker<bool>(worker, dg, patterns, process);
            }// else if (work_type_msg.agg_value_type == AggValueType::Domain) {
            //  const auto view = [](auto &&v) { return v.get_support(); };
            //  match_single_distributed_worker<struct Domain>(worker, dg, patterns, process);
            //} else if (work_type_msg.agg_value_type == AggValueType::DiscoveryDomain) {
            //  const auto view = [](auto &&v) { return v.get_support(); };
            //  match_single_distributed_worker<struct DiscoveryDomain>(worker, dg, patterns, process);
            //}
          }
        }
      } else {
        utils::Log{} << "Worker received unexepected message of type " << pg_msg.msg_type << "\n";
      }
    }
    return;
  }

  struct Master
  create_master(size_t nworkers)
  {
    struct Master master;

    zmq::context_t ctx;
    master.context = std::move(ctx);
    master.nworkers = nworkers;
    master.data_graph = std::make_unique<DataGraph>("data/citeseer");

    // TODO: set number of threads for master
    // TODO: set data_graph_name

    master.sock = std::make_unique<zmq::socket_t>(master.context, zmq::socket_type::router);
    master.sock->set(zmq::sockopt::linger, 0);
    master.sock->set(zmq::sockopt::router_mandatory, 1);

    master.sock->bind("tcp://*:9999");

    uint32_t num_workers_ready = 0;
    utils::Log{} << "Waiting for all workers to become ready...\n";
    while (num_workers_ready < nworkers) {
      zmq::multipart_t ready_msg;
      const auto res = ready_msg.recv(*master.sock, 0);
      if (res) {
        std::string identity = ready_msg.popstr();
        master.worker_ids.push_back(identity);
        num_workers_ready++;
        utils::Log{} << "[" << num_workers_ready << "/" << nworkers << "] workers are ready" << "\n";
      }
    }
    return master;
  }
} // namespace Peregrine

#endif
