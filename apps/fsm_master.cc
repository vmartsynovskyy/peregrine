#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Peregrine.hh"

#include "Domain.hh"

int main(int argc, char *argv[])
{
  if (argc < 4)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <max size> <support threshold> [# threads]" << std::endl;
    return -1;
  }


  const std::string data_graph_name(argv[1]);
  uint32_t k = std::stoi(argv[2]);

  uint64_t threshold = std::stoi(argv[3]);
  size_t nworkers = argc < 5 ? std::thread::hardware_concurrency() : std::stoi(argv[4]);

  const auto view = [](auto &&v) { return v.get_support(); };

  std::vector<uint64_t> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  Peregrine::Master master = Peregrine::create_master(nworkers, data_graph_name);

  std::cout << k << "-FSM with threshold " << threshold << std::endl;

  // initial discovery
  auto t1 = utils::get_timestamp();
  {
    std::vector<Peregrine::SmallGraph> patterns = {Peregrine::PatternGenerator::star(3)};
    patterns.front().set_labelling(Peregrine::Graph::DISCOVER_LABELS);
    auto psupps = Peregrine::match_distributed<Peregrine::Pattern, struct DiscoveryDomain>(master, patterns, Peregrine::default_master_process, view);
    for (const auto &[p, supp] : psupps)
    {
      if (supp >= threshold)
      {
        freq_patterns.push_back(p);
        supports.push_back(supp);
      }
    }
  }

  std::vector<Peregrine::SmallGraph> patterns = Peregrine::PatternGenerator::extend(freq_patterns, Peregrine::PatternGenerator::EDGE_BASED);

  uint32_t step = 2;
  while (step < k && !patterns.empty())
  {
    freq_patterns.clear();
    supports.clear();
    auto psupps = Peregrine::match_distributed<Peregrine::Pattern, struct Domain>(master, patterns, Peregrine::default_master_process, view);

    for (const auto &[p, supp] : psupps)
    {
      if (supp >= threshold)
      {
        freq_patterns.push_back(p);
        supports.push_back(supp);
      }
    }

    patterns = Peregrine::PatternGenerator::extend(freq_patterns, Peregrine::PatternGenerator::EDGE_BASED);
    step += 1;
  }
  auto t2 = utils::get_timestamp();

  std::cout << freq_patterns.size() << " frequent patterns: " << std::endl;
  for (uint32_t i = 0; i < freq_patterns.size(); ++i)
  {
    std::cout << freq_patterns[i].to_string() << ": " << supports[i] << std::endl;
  }

  std::cout << "finished in " << (t2-t1)/1e6 << "s" << std::endl;
  return 0;
}

