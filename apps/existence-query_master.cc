#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>

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
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-clique> [# workers]" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t num_workers = argc > 3 ? std::stoi(argv[3]) : 1;

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

  Peregrine::Master master = Peregrine::create_master(num_workers, data_graph_name);

  const auto process = [](auto &&agg_value) {
    return agg_value.get_support();
  };

  std::vector<std::pair<Peregrine::SmallGraph, bool>> results = Peregrine::match_distributed<Peregrine::Pattern, bool>(master, patterns, process);

  std::cout << display_name;
  if (results.front().second) std::cout << " exists in " << data_graph_name << std::endl;
  else std::cout << " doesn't exist in " << data_graph_name << std::endl;

  master.sock->close();
  master.context.close();

  return 0;
}

