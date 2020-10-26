#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Peregrine.hh"

bool is_directory(const std::string &path)
{
   struct stat statbuf;
   if (stat(path.c_str(), &statbuf) != 0)
       return 0;
   return S_ISDIR(statbuf.st_mode);
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> <# threads> <master|hostname-of-master for workers> [# workers]" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nthreads = std::stoi(argv[3]);
  bool is_parallel = argc > 4;
  bool is_master = is_parallel ? std::string(argv[4]) == "master" : false;
  std::string master_host = !is_master && argc > 4 ? std::string(argv[4]) : "127.0.0.1";
  size_t num_workers = argc > 5 ? std::stoi(argv[5]) : 1;

  std::vector<Peregrine::SmallGraph> patterns;

  std::string display_name;
  if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
    display_name = std::to_string(k) + "-clique";
  }
  else
  {
    patterns.emplace_back(pattern_name);
    display_name = patterns.front().to_string();
  }

  std::vector<std::pair<Peregrine::SmallGraph, bool>> results;

  if (is_parallel) {
    const auto process = [](auto &&a, auto &&cm) { a.map(cm.pattern, true); /* a.stop(); */ };
    results = Peregrine::match_parallel<Peregrine::Pattern, bool>(data_graph_name, patterns, nthreads, num_workers, is_master, master_host, process);
  } else {
    const auto process = [](auto &&a, auto &&cm) { a.map(cm.pattern, true); a.stop(); };
    results = Peregrine::match<Peregrine::Pattern, bool, Peregrine::ON_THE_FLY, Peregrine::STOPPABLE>(data_graph_name, patterns, nthreads, process);
  }

  std::cout << display_name;
  if (results.front().second) std::cout << " exists in " << data_graph_name << std::endl;
  else std::cout << " doesn't exist in " << data_graph_name << std::endl;

  return 0;
}
