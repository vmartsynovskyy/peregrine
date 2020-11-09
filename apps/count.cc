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
  if (auto end = pattern_name.rfind("motifs"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns = Peregrine::PatternGenerator::all(k,
        Peregrine::PatternGenerator::VERTEX_BASED,
        Peregrine::PatternGenerator::INCLUDE_ANTI_EDGES);
  }
  else if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
  }
  else
  {
    patterns.emplace_back(pattern_name);
  }

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  if (is_directory(data_graph_name))
  {
    result = Peregrine::count(data_graph_name, patterns, nthreads);
  }
  else
  {
    Peregrine::SmallGraph G(data_graph_name);
    result = Peregrine::count(G, patterns, nthreads);
  }

  for (const auto &[p, v] : result)
  {
    std::cout << p << ": " << v << std::endl;
  }

  return 0;
}
