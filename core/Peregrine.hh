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

namespace Peregrine
{
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

  template <typename AggKeyT, typename AggValueT, typename VF>
  void
  match_multi_master(
      DataGraph *dg,
      const std::vector<SmallGraph> &patterns,
      uint32_t nworkers,
      std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> &results,
      VF &viewer
  )
  {
    // initialize
    if (patterns.empty()) return;
    std::vector<std::unordered_map<AggKeyT, AggValueT>> results_map(patterns.size());

    utils::Log{} << "---- MASTER ----\n";

    zmq::context_t ctx;

    // blocks until workers are all ready
    auto [master_pull_sock, master_push_sock] = create_master_sockets(ctx, nworkers);

    auto t1 = utils::get_timestamp();
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    for (uint32_t i = 0; i < (uint32_t) patterns.size(); i++) {
      AnalyzedPattern ap(patterns[i]);
      uint32_t vgs_count = ap.vgs.size();
      uint32_t num_vertices = dg->get_vertex_count();
      uint64_t num_tasks = vgs_count * num_vertices;
      uint64_t num_tasks_per_item = 1024;

      uint64_t task_item_start = 0;
      while (task_item_start < num_tasks) {
        send_task_msg(*master_push_sock, task_item_start, std::min(task_item_start + num_tasks_per_item, num_tasks + 1), tasks_sent, i);
        tasks_sent++;

        task_item_start += num_tasks_per_item;
      }
    }

    utils::Log{} << "master done sending tasks\n";

    zmq::message_t task_complete_msg;
    while (tasks_completed < tasks_sent && master_pull_sock->recv(task_complete_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_complete_pg_msg = static_cast<struct PeregrineMessage *>(task_complete_msg.data());

      uint32_t pattern_idx = task_complete_pg_msg->msg.comp_m_task.pattern_idx;

      std::unordered_map<AggKeyT, AggValueT> agg_result;
      char *serial_data = static_cast<char *>(task_complete_msg.data()) + sizeof(struct PeregrineMessage);
      size_t serial_data_size = task_complete_msg.size() - sizeof(struct PeregrineMessage);
      std::stringstream ss(std::string(serial_data, serial_data_size));
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
    utils::Log{} << "master done receiving tasks\n";

    for (size_t i = 0; i < patterns.size(); i++) {
      auto pattern_results_map = results_map[i];
      auto pattern = patterns[i];
      for (auto &[k, v] : pattern_results_map) {
        results.emplace_back(SmallGraph(pattern, k), viewer(v));
      }
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(*master_pull_sock, *master_push_sock, nworkers);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename AggValueT, typename VF>
  void
  match_vector_master(
      DataGraph *dg,
      const std::vector<SmallGraph> &patterns,
      uint32_t nworkers,
      std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> &results,
      VF &viewer
  )
  {
    // initialize
    if (patterns.empty()) return;
    std::vector<std::vector<AggValueT>> results_map;

    utils::Log{} << "---- MASTER ----\n";

    zmq::context_t ctx;

    // blocks until workers are all ready
    auto [master_pull_sock, master_push_sock] = create_master_sockets(ctx, nworkers);

    // calculated same way as in VecAggregator
    uint32_t vec_agg_offset = dg->get_label_range().first;
    uint32_t vec_agg_size = dg->get_label_range().second - vec_agg_offset + 1;

    auto t1 = utils::get_timestamp();
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    for (uint32_t i = 0; i < (uint32_t) patterns.size(); i++) {
      AnalyzedPattern ap(patterns[i]);
      std::vector<AggValueT> inner_results_map;
      for (uint32_t j = 0; j < vec_agg_size; j++) {
        inner_results_map.emplace_back(ap.num_aut_sets(), true);
      }
      results_map.push_back(inner_results_map);
      uint32_t vgs_count = ap.vgs.size();
      uint32_t num_vertices = dg->get_vertex_count();
      uint64_t num_tasks = vgs_count * num_vertices;
      uint64_t num_tasks_per_item = 1024;

      uint64_t task_item_start = 0;
      while (task_item_start < num_tasks) {
        send_task_msg(*master_push_sock, task_item_start, std::min(task_item_start + num_tasks_per_item, num_tasks + 1), tasks_sent, i);
        tasks_sent++;

        task_item_start += num_tasks_per_item;
      }
    }

    utils::Log{} << "master done sending tasks\n";

    zmq::message_t task_complete_msg;
    while (tasks_completed < tasks_sent && master_pull_sock->recv(task_complete_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_complete_pg_msg = static_cast<struct PeregrineMessage *>(task_complete_msg.data());

      uint32_t pattern_idx = task_complete_pg_msg->msg.comp_m_task.pattern_idx;

      std::vector<AggValueT> agg_result;
      char *serial_data = static_cast<char *>(task_complete_msg.data()) + sizeof(struct PeregrineMessage);
      size_t serial_data_size = task_complete_msg.size() - sizeof(struct PeregrineMessage);
      std::stringstream ss(std::string(serial_data, serial_data_size));
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
    utils::Log{} << "master done receiving tasks\n";

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
    stop_workers(*master_pull_sock, *master_push_sock, nworkers);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  template <typename AggValueT, typename VF>
  void
  match_single_master(
      DataGraph *dg,
      const std::vector<SmallGraph> &patterns,
      uint32_t nworkers,
      std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> &results,
      VF &viewer
  )
  {
    // initialize
    if (patterns.empty()) return;
    std::vector<AggValueT> results_map;

    utils::Log{} << "---- MASTER ----\n";

    zmq::context_t ctx;

    // blocks until workers are all ready
    auto [master_pull_sock, master_push_sock] = create_master_sockets(ctx, nworkers);

    auto t1 = utils::get_timestamp();
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    for (uint32_t i = 0; i < (uint32_t) patterns.size(); i++) {
      AnalyzedPattern ap(patterns[i]);
      results_map.emplace_back(ap.num_aut_sets(), true);
      uint32_t vgs_count = ap.vgs.size();
      uint32_t num_vertices = dg->get_vertex_count();
      uint64_t num_tasks = vgs_count * num_vertices;
      uint64_t num_tasks_per_item = 1024;

      uint64_t task_item_start = 0;
      while (task_item_start < num_tasks) {
        send_task_msg(*master_push_sock, task_item_start, std::min(task_item_start + num_tasks_per_item, num_tasks + 1), tasks_sent, i);
        tasks_sent++;

        task_item_start += num_tasks_per_item;
      }
    }

    utils::Log{} << "master done sending tasks\n";

    zmq::message_t task_complete_msg;
    while (tasks_completed < tasks_sent && master_pull_sock->recv(task_complete_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_complete_pg_msg = static_cast<struct PeregrineMessage *>(task_complete_msg.data());

      uint32_t pattern_idx = task_complete_pg_msg->msg.comp_m_task.pattern_idx;

      AggValueT agg_result;
      char *serial_data = static_cast<char *>(task_complete_msg.data()) + sizeof(struct PeregrineMessage);
      size_t serial_data_size = task_complete_msg.size() - sizeof(struct PeregrineMessage);
      std::stringstream ss(std::string(serial_data, serial_data_size));
      {
        cereal::BinaryInputArchive archive(ss);
        archive(agg_result);
      }

      results_map[pattern_idx] += agg_result;

      tasks_completed++;
    }
    utils::Log{} << "master done receiving tasks\n";

    for (size_t i = 0; i < patterns.size(); i++) {
      auto p = patterns[i];
      results.emplace_back(p, viewer(results_map[i]));
    }
    auto t2 = utils::get_timestamp();

    // tell all workers that job is done
    stop_workers(*master_pull_sock, *master_push_sock, nworkers);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
  }

  void
  count_master(
      DataGraph *dg,
      const std::vector<SmallGraph> &patterns,
      const std::vector<SmallGraph> &new_patterns,
      uint32_t nworkers,
      bool must_convert_counts,
      std::vector<std::pair<SmallGraph, uint64_t>> &results
  )
  {
    // initialize
    if (patterns.empty()) return;
    std::vector<uint64_t> results_map(new_patterns.size(), 0);

    utils::Log{} << "---- MASTER ----\n";

    zmq::context_t ctx;

    auto master_sock_pair = create_master_sockets(ctx, nworkers);
    auto master_pull_sock = std::move(master_sock_pair.first);
    auto master_push_sock = std::move(master_sock_pair.second);

    auto t1 = utils::get_timestamp();
    // send tasks now that workers are ready
    uint64_t tasks_sent = 0;
    uint64_t tasks_completed = 0;
    for (uint32_t i = 0; i < (uint32_t) new_patterns.size(); i++) {
      AnalyzedPattern ap(new_patterns[i]);
      uint32_t vgs_count = ap.vgs.size();
      uint32_t num_vertices = dg->get_vertex_count();
      uint64_t num_tasks = vgs_count * num_vertices;
      uint64_t num_tasks_per_item = 1024;

      uint64_t task_item_start = 0;
      while (task_item_start < num_tasks) {
        send_task_msg(*master_push_sock, task_item_start, std::min(task_item_start + num_tasks_per_item, num_tasks + 1), tasks_sent, i);
        tasks_sent++;

        task_item_start += num_tasks_per_item;
      }
    }

    utils::Log{} << "master done sending tasks\n";

    zmq::message_t task_complete_msg;
    while (tasks_completed < tasks_sent && master_pull_sock->recv(task_complete_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_complete_pg_msg = static_cast<struct PeregrineMessage *>(task_complete_msg.data());
      results_map[task_complete_pg_msg->msg.comp_c_task.pattern_idx] += task_complete_pg_msg->msg.comp_c_task.count;

      tasks_completed++;
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
    stop_workers(*master_pull_sock, *master_push_sock, nworkers);

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "master all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return;
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
  match_parallel(DataGraphT &&data_graph,
      const std::vector<SmallGraph> &patterns,
      size_t nthreads,
      size_t nworkers,
      bool is_master,
      std::string master_host,
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

      auto result = match_single_parallel<AggValueT>(process, view, nthreads, data_graph, single, is_master, nworkers, master_host);
      if (!vector.empty()) {
        utils::Log{} << "WARN: results will be incomplete, match_vector_parallel is not yet implemented\n";
      }
      if (!multi.empty()) {
        utils::Log{} << "WARN: results will be incomplete, match_multi_parallel is not yet implemented\n";
      }
      auto vector_result = match_vector_parallel<AggValueT>(process, view, nthreads, data_graph, vector, is_master, nworkers, master_host);
      auto multi_result = match_multi_parallel<AggKeyT, AggValueT>(process, view, nthreads, data_graph, multi, is_master, nworkers, master_host);

      result.insert(result.end(), vector_result.begin(), vector_result.end());
      result.insert(result.end(), multi_result.begin(), multi_result.end());

      return result;
    }
    else
    {
      return match_multi_parallel<AggKeyT, AggValueT>(process, view, nthreads, data_graph, patterns, is_master, nworkers, master_host);
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

  template <typename AggKeyT, typename AggValueT, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_multi_parallel
  (PF &&process, VF &&viewer, size_t nthreads, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns, bool is_master, size_t nworkers, std::string master_host)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    // initialize
    Barrier barrier(nthreads);
    std::vector<std::thread> pool;
    std::thread master;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    zmq::context_t ctx;

    auto [worker_pull_sock, worker_push_sock] = create_worker_sockets(ctx, master_host);

    if (is_master) {
      master = std::thread(match_multi_master<AggKeyT, AggValueT, decltype(viewer)>, &dg, std::ref(patterns), nworkers, std::ref(results), std::ref(viewer)); 
    }

    MapAggregator<AggKeyT, AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(nworkers, viewer);

    for (uint8_t i = 0; i < nthreads; ++i)
    {
      //auto &ah = aggregator.get_handle(i);
      pool.emplace_back(map_worker_parallel<
            AggKeyT,
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

    auto t1 = utils::get_timestamp();
    utils::timestamp_t working_t_sum = 0;
    uint64_t worker_tasks_completed = 0;
    zmq::message_t task_msg;
    while (worker_pull_sock->recv(task_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_pg_msg = static_cast<struct PeregrineMessage *>(task_msg.data());
      if (task_pg_msg->msg_type == MessageType::JobDone) {
        worker_push_sock->send(task_msg, zmq::send_flags::none);
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg->msg.task.start_task;
      Context::task_end = task_pg_msg->msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg->msg.task.pattern_idx;
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
      worker_tasks_completed++;

      send_match_task_complete_msg<AggKeyT, AggValueT>(*worker_push_sock, &task_pg_msg->msg.task, aggregator.global);
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

  template <typename AggKeyT, typename AggValueT>
  zmq::send_result_t
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

    return socket.send(zmq_msg, zmq::send_flags::dontwait);
  }

  template <typename AggValueT>
  zmq::send_result_t
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

    return socket.send(zmq_msg, zmq::send_flags::dontwait);
  }

  template <typename AggValueT>
  zmq::send_result_t
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

    return socket.send(zmq_msg, zmq::send_flags::dontwait);
  }

  template <typename AggValueT, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_single_parallel
  (PF &&process, VF &&viewer, size_t nthreads, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns, bool is_master, size_t nworkers, std::string master_host)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    // initialize
    Barrier barrier(nthreads);
    std::vector<std::thread> pool;
    std::thread master;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    zmq::context_t ctx;

    auto [worker_pull_sock, worker_push_sock] = create_worker_sockets(ctx, master_host);

    if (is_master) {
      master = std::thread(match_single_master<AggValueT, decltype(viewer)>, &dg, std::ref(patterns), nworkers, std::ref(results), std::ref(viewer)); 
    }

    SVAggregator<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(nthreads, viewer);

    for (uint8_t i = 0; i < nthreads; ++i)
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

    zmq::message_t task_msg;

    utils::timestamp_t working_t_sum = 0;
    uint64_t worker_tasks_completed = 0;
    auto t1 = utils::get_timestamp();
    while (worker_pull_sock->recv(task_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_pg_msg = static_cast<struct PeregrineMessage *>(task_msg.data());
      if (task_pg_msg->msg_type == MessageType::JobDone) {
        worker_push_sock->send(task_msg, zmq::send_flags::none);
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg->msg.task.start_task;
      Context::task_end = task_pg_msg->msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg->msg.task.pattern_idx;
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

      send_match_task_complete_msg(*worker_push_sock, &task_pg_msg->msg.task, aggregator.global);
      worker_tasks_completed++;
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    if (is_master) {
      master.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

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

  template <typename AggValueT, typename DataGraphT, typename PF, typename VF>
  std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>>
  match_vector_parallel
  (PF &&process, VF &&viewer, size_t nthreads, DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns, bool is_master, size_t nworkers, std::string master_host)
  {
    std::vector<std::pair<SmallGraph, decltype(std::declval<VF>()(std::declval<AggValueT>()))>> results;

    if (patterns.empty()) return results;

    // initialize
    Barrier barrier(nthreads);
    std::vector<std::thread> pool;
    std::thread master;
    DataGraph dg(std::move(data_graph));
    dg.set_rbi(patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(patterns);
    Context::data_graph = &dg;
    Context::current_pattern = std::make_shared<AnalyzedPattern>(AnalyzedPattern(dg.rbi));

    zmq::context_t ctx;

    auto [worker_pull_sock, worker_push_sock] = create_worker_sockets(ctx, master_host);

    if (is_master) {
      master = std::thread(match_vector_master<AggValueT, decltype(viewer)>, &dg, std::ref(patterns), nworkers, std::ref(results), std::ref(viewer)); 
    }

    VecAggregator<AggValueT, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE, decltype(viewer)> aggregator(nworkers, viewer);

    for (uint8_t i = 0; i < nthreads; ++i)
    {
      //auto &ah = aggregator.get_handle(i);
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

    auto t1 = utils::get_timestamp();
    utils::timestamp_t working_t_sum = 0;
    uint64_t worker_tasks_completed = 0;
    zmq::message_t task_msg;
    while (worker_pull_sock->recv(task_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_pg_msg = static_cast<struct PeregrineMessage *>(task_msg.data());
      if (task_pg_msg->msg_type == MessageType::JobDone) {
        worker_push_sock->send(task_msg, zmq::send_flags::none);
        break;
      }

      auto working_t1 = utils::get_timestamp();

      // reset state
      Context::task_ctr = task_pg_msg->msg.task.start_task;
      Context::task_end = task_pg_msg->msg.task.end_task;

      // set new pattern
      Context::pattern_idx = task_pg_msg->msg.task.pattern_idx;
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
      worker_tasks_completed++;

      send_match_task_complete_msg(*worker_push_sock, &task_pg_msg->msg.task, aggregator.global);
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

    return results;
  }

  zmq::send_result_t
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

    zmq::message_t zmq_msg(&pg_completed_task_msg, sizeof(pg_completed_task_msg));
    return socket.send(zmq_msg, zmq::send_flags::dontwait);
  }

  template <typename DataGraphT>
  std::vector<std::pair<SmallGraph, uint64_t>>
  count_parallel(DataGraphT &&data_graph, const std::vector<SmallGraph> &patterns, size_t nthreads, bool is_master, size_t nworkers, std::string master_host)
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

    Barrier barrier(nthreads);
    std::thread master;
    std::vector<std::thread> pool;
    DataGraph dg(data_graph);
    dg.set_rbi(new_patterns.front());

    utils::Log{} << "Finished reading datagraph: |V| = " << dg.get_vertex_count()
              << " |E| = " << dg.get_edge_count()
              << "\n";

    dg.set_known_labels(new_patterns);

    zmq::context_t ctx;

    auto worker_sock_pair = create_worker_sockets(ctx, master_host);
    auto worker_pull_sock = std::move(worker_sock_pair.first);
    auto worker_push_sock = std::move(worker_sock_pair.second);

    if (is_master) {
      master = std::thread(count_master, &dg, std::ref(patterns), std::ref(new_patterns), nworkers, must_convert_counts, std::ref(results)); 
    }

    for (uint8_t i = 0; i < nthreads; ++i)
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
    zmq::message_t task_msg;
    uint64_t worker_tasks_completed = 0;
    while (worker_pull_sock->recv(task_msg, zmq::recv_flags::none).has_value()) {
      struct PeregrineMessage *task_pg_msg = static_cast<struct PeregrineMessage *>(task_msg.data());
      if (task_pg_msg->msg_type == MessageType::JobDone) {
        worker_push_sock->send(task_msg, zmq::send_flags::none);
        break;
      }

      auto working_t1 = utils::get_timestamp();
      Context::gcount = 0;

      Context::task_ctr = task_pg_msg->msg.task.start_task;
      Context::task_end = task_pg_msg->msg.task.end_task;
      Context::pattern_idx = task_pg_msg->msg.task.pattern_idx;
      dg.set_rbi(new_patterns[Context::pattern_idx]);

      barrier.release();

      barrier.join();

      uint64_t global_count = Context::gcount;
      auto working_t2 = utils::get_timestamp();
      working_t_sum += (working_t2 - working_t1);

      send_task_complete_msg(*worker_push_sock, &task_pg_msg->msg.task, global_count);
      worker_tasks_completed++;
    }
    utils::Log{} << "worker is done all tasks in queue after completing " << worker_tasks_completed << " tasks (" << working_t_sum/1e6 << "s of working time)\n";

    if (is_master) {
      master.join();
    }
    auto t2 = utils::get_timestamp();

    barrier.finish();
    for (auto &th : pool)
    {
      th.join();
    }

    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    return results;
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
} // namespace Peregrine

#endif
